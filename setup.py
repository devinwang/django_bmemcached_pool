from setuptools import setup
setup(
    name='django-bmemcached_pool',
    version='0.1',
    author='Devin Wang',
    author_email='gnujava@gmail.com',
    description='A Django cache backend to use bmemcached module and support pool.',
    url='https://github.com/devinwang/django_bmemcached_pool',
    packages=['django_bmemcached_pool'],
    classifiers=[
        'Development Status :: 1 - alpha',
        'License :: GPL 3',
        'Operating System :: OS Independent',
    ]
)
