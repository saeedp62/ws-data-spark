from setuptools import setup

try:    # for pip >= 10
    from pip._internal.req import parse_requirements
except ImportError:     # for pip <= 9.0.3
    from pip.req import parse_requirements

setup(
    name='POI_Analysis',
    version=1.0,
    description='Analysis of sample data of POIs',
    author='Saeed Pouryazdian',
    author_email='pouryazdian.saeed@gmail.com',
    setup_requires=['pytest-runner'],
    include_package_data=True,
    packages=['ws-data-spark'],
    url='https://github.com/saeedp62/ws-data-spark',
    install_reqs=[str(req.req) for req in parse_requirements('./requirements.txt', session='hack')],
)
