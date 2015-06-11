avro-golang-compiler + golang-avro
==================================

avro-golang-compiler is a fork of the Avro java object compiler.
It generates golang code from Avro AVDL files that can be used to
serialize/deserialize Avro objects.

This generates code from .avdl files, as documented [here](http://avro.apache.org/docs/1.7.7/idl.html).

An example of the generated code can be found on [github](https://github.com/BBN-D/avro-golang-example).

Requirements
============

* Java
* Avro C bindings must be installed
  * If you use maps with embedded records, make sure you have this [patch](https://issues.apache.org/jira/browse/AVRO-1561) applied
    otherwise you will get null pointer exceptions

Usage
=====

Build the package using maven using the following command

	mvn clean package dependency:copy-dependencies

There is a static java class that can be used to generate golang code.
It was adapted from the avro maven plugin, and with a little work one
could make a working maven plugin. run.sh is a simple wrapper that will
run this class:

	./run.sh [avdl directory] [output directory] [go import path for output]

For example, if the repo that you're generating is located at https://github.com/foo/bar,
and you wish to place your code in the 'myavro' path in that repository, you
would run this:

    ./run.sh ~/path/to/avdl ~/path/to/foo/bar/myavro github.com/foo/bar/myavro

The generated code also autogenerates golang unit tests that can be ran using
the `go test` utility.

Questions & Answers
===================

### Why not a golang based compiler?

* This should make it easier for the avro project to adapt it as it will 
  take few changes to integrate into their existing project
* If you're using avro you're probably using it to interact with something
  that is using java anyways

### Why does this use the avro C bindings instead of native golang?

When this was originally written, a golang avro implementation did not exist,
and we felt this would be less work, as the C bindings are already well
tested/supported by the avro project. 

Feel free to adapt this code to use a golang avro implementation. The code
generator was written such that a native golang avro implementation should
be pretty easy to plug in. 

### Why aren't the avro wrappers distributed separately from the codegen?

Didn't want to have to deal with versioning/import issues that come up
when using golang. The generated code is tightly bound to the underlying
avro wrappers, so didn't make sense to distribute them separately.

Quirks
======

* Maps and arrays underneath a union type always use interface{} as the
  value type, as opposed to the actual type.
* Currently does not properly support avro linked schemas in unions (though
  it appears that the C API does not always properly support this either)
* Some really complex schemas are not supported (but the generated tests will fail)

Legal
=====

This code has been approved by DARPA's Public Release Center for
public release, with the following statement:

* Approved for Public Release, Distribution Unlimited

Copyright & License
-------------------

Copyright (c) 2014-2015 Raytheon BBN Technologies Corp

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
