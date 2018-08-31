from setuptools import setup, find_packages

with open('README.rst') as f:
    long_description = f.read()

version = '0.1'

setup(
    name='nyctlc',
    version=version,
    packages=find_packages(),
    url='https://github.com/djsaunde/nyctlc',
    license='MIT',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    author='Daniel Saunders',
    author_email='djsaunde@cs.umass.edu',
    description='Simple Python package for processing NYC\'s Taxi and Limousine Commission taxi trip records.',
    zip_safe=False,
    download_url='https://github.com/Hananel-Hazan/bindsnet/archive/%s.tar.gz' % version,
    install_requires=[
        'setuptools>=39.0.1',
        'dask>=0.18.1'
    ]
)
