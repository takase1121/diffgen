from setuptools import setup, find_packages

setup(
  name='diffgen',
  version='0.0.1',
  license='MIT',
  description='A tool to generate directory diff',
  package_dir={'': 'src'},
  packages=find_packages(where='diffgen'),
  python_requires='>=3.5',
  entry_points={
    'console_scripts': ['diffgen=diffgen:main']
  }
)
