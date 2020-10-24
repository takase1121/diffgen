#!/usr/bin/env python

import io
import json
import pathlib
import hashlib
import argparse
from unqlite import UnQLite
from tqdm import tqdm

class FileInfo:
  """Contains metadata and md5 of a file"""
  md5 = ''
  name = ''
  size = 0
  mtime = 0

  @staticmethod
  def get_md5(file, blksize):
    """Getting md5 hash of the file"""
    hash = hashlib.new('md5', usedforsecurity=False)
    with file.open('rb') as f:
      while True:
        data = f.read(blksize)
        if data:
          hash.update(data)
        else:
          break
    return hash.hexdigest()

  def __init__(self, file, root, pool=None):
    stat_result = file.stat()
    self.name = file.relative_to(root).as_posix()
    self.mtime = stat_result.st_mtime
    self.size = stat_result.st_size

    blksize = stat_result.st_blksize if 'st_blksize' in stat_result else io.DEFAULT_BUFFER_SIZE
    if pool:
      self.md5 = pool.apply_async(FileInfo.get_md5, (file, blksize))
    else:
      self.md5 = FileInfo.get_md5(file, blksize)


def generate_dir_listing(root, depth, pool=None, ignored_patterns=[]):
  """
  Recursively generate FileInfo until a specified depth.
  depth - max depth (inclusive)
  pool - multiprocessing Pool if your disk can handle the random reads, for HDD, this is not recommended
  ignored_patterns - ignored patterns, passed to Pathlib.match
  """
  progress = tqdm(desc='Generating file list', dynamic_ncols=True, unit='file')
  def process_dir(path, depth, current_depth):
    if current_depth >= depth:
      return

    for f in path.iterdir():
      try:
        if any([f.match(pattern) for pattern in ignored_patterns]):
          continue
        if f.is_file():
          progress.set_description(f'File: {f.name[:32]}...')
          yield FileInfo(f, root, pool)
          progress.update()
        elif f.is_dir():
          yield from process_dir(f, depth, current_depth + 1)
        else:
          pass
      except:
        continue

  yield from process_dir(root, depth, 0)

def dump_info(dirinfo, out_path):
  """Dump FileInfo into a UnQLite database"""
  out_path.unlink(missing_ok=True)
  with UnQLite(out_path.as_posix()) as db:
    with db.transaction():
      for info in dirinfo:
        db[info.name] = json.dumps(info.__dict__)

def get_diff(dirinfo, db_path):
  """Yield entries that are different in the database (or not in it)"""
  with UnQLite(db_path.as_posix()) as db:
    for info in dirinfo:
      if info.name in db:
        dbinfo = json.loads(db[info.name])
        if (info.size != dbinfo['size'] and
            info.mtime != dbinfo['mtime'] and
            info.md5 != dbinfo['md5']):
          yield info
      else:
        yield info

def dump_diff(diff, root, out_path):
  """Dump diff into a file"""
  with out_path.open('w') as f:
    f.writelines(map(lambda x: (root / x.name).as_posix() + '\n', diff))

def main_generate(args):
  """main() for generate subcommand"""
  sourcepath = pathlib.Path(args.sourcepath)
  outpath = pathlib.Path(args.outpath)
  if args.process != 0:
    with Pool(args.process) as pool:
      listing = generate_dir_listing(sourcepath, args.depth, pool, args.ignore)
      dump_info(listing, outpath)
  else:
    listing = generate_dir_listing(sourcepath, args.depth, ignored_patterns=args.ignore)
    dump_info(listing, outpath)

def main_diff(args):
  """main() for diff subcommand"""
  sourcepath = pathlib.Path(args.sourcepath)
  dbpath = pathlib.Path(args.dbpath)
  outpath = pathlib.Path(args.outpath)
  if args.process != 0:
    with Pool(args.process) as pool:
      listing = generate_dir_listing(sourcepath, args.depth, pool, args.ignore)
      diff = get_diff(listing, dbpath)
      dump_diff(diff, sourcepath, outpath)
  else:
    listing = generate_dir_listing(sourcepath, args.depth, ignored_patterns=args.ignore)
    diff = get_diff(listing, dbpath)
    dump_diff(diff, sourcepath, outpath)


def main():
  """The actual main()"""
  parser = argparse.ArgumentParser()
  subparsers = parser.add_subparsers()

  parser.add_argument('--process', type=int, default=0,
                      help='# Processes to use for MD5 generation')
  parser.add_argument('--depth', type=int, default=1,
                      help='Max depth to traverse, can be arbitarily large (eg. 9999)')
  parser.add_argument('--ignore', action='append', default=[],
                      help='Ignore certain glob patterns (eg. "a/*.py")')
  parser.set_defaults(func=lambda args: parser.print_help())

  parser_generate = subparsers.add_parser('generate',
                                          help='Generates a diff database')
  parser_generate.add_argument('sourcepath',
                                help='Source path')
  parser_generate.add_argument('outpath',
                                help='Output path')
  parser_generate.set_defaults(func=main_generate)

  parser_diff = subparsers.add_parser('diff',
                                      help='Generates a list of files that had changed since last diff')
  parser_diff.add_argument('sourcepath',
                            help='Source path')
  parser_diff.add_argument('dbpath',
                            help='Diff database path')
  parser_diff.add_argument('outpath',
                            help='Output path')
  parser_diff.set_defaults(func=main_diff)

  args = parser.parse_args()
  args.func(args)
