from setuptools import setup

setup(
    name = "restoredb",
    version = "1.1",
    author = "Cecile Tonglet",
    author_email = "cecile.tonglet@gmail.com",
    description = ("Database restoration script to automatically decompress "
                   "any format and pipe to the right restorer"),
    license = "MIT",
    keywords = "postgres database dump restore",
    url = "https://github.com/cecton/restoredb.py",
    py_modules = ['restoredb'],
    scripts = ['restoredb.py'],
    install_requires = ['StreamDecompressor'],
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
    ],
)
