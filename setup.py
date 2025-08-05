from setuptools import setup, find_packages

with open("requirements.txt") as f:
    requirements = f.read().splitlines()

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name = "catnip_v3",
    version = "0.0.001",
    author = "wintonm-fla",
    author_email = "wintonm@floridapanthers.com",
    description = "Facilitating repeatable internal code",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/wintonm-fla/Catnip_v3",
    license = "MIT",
    packages = find_packages(),
    install_requires = requirements
)