import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

from pgseqmover import VERSION

setuptools.setup(
    name='pg-sequence-increaser',
    version=VERSION,
    author='Lev Kokotov',
    author_email='lev.kokotov@instacart.com',
    description="Set all sequences to a higher value when promoting a PostgreSQL logical replica",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/levkk/pg-sequence-increaser',
    install_requires=[
        'Click>=7.0',
        'colorama>=0.4.3',
        'psycopg2>=2.8.4',
    ],
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent', # Colorama!
    ],
    python_requires='>=3',
    entry_points={
        'console_scripts': [
            'pgseqmover = pgseqmover:cli',
        ]
    },
)