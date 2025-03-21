from setuptools import setup, find_packages

# Load requirements from requirements.txt
with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='workflow_director',
    version='0.1.0',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,  # Include additional files specified in MANIFEST.in
)