Building Elasticsearch with Gradle
=============================

Elasticsearch is built using the [Gradle](https://gradle.org/) open source build tools.

This document provides a general guidelines for using and working on the elasticsearch build logic.

## Build logic organisation

The elasticsearch project contains 3 build related projects that are included into the elasticsearch build as a [composite build](https://docs.gradle.org/current/userguide/composite_builds.html).

### `build-conventions`

This project contains build conventions that are applied to all elasticsearch projects.

### `build-tools`

This project contains all build logic that we publish for third party elasticsearch plugin authors.
We provide the following plugins:

- `elasticsearch.esplugin` - A gradle plugin for building an elasticsearch plugin.
- `elasticsearch.testclusters` - A gradle plugin for setting up es clusters for testing within a build.

This project is published as part of the elasticsearch release and accessible by
`org.elasticsearch.gradle:build-tools:<versionNumber>`.
These build tools are also used by the `elasticsearch-hadoop` project maintained by elastic.

### `build-tools-internal`

This project contains all elasticsearch project specific build logic that is not meant to be shared
with other internal or external projects.

## Build guidelines

This is an intentionally small set of guidelines to build users and authors
to ensure we keep the build consistent. We also publish elasticsearch build logic
as `build-tools` to be usuable by thirdparty elasticsearch plugin authors. This is
also used by other elastic teams like `elasticsearch-hadoop`.
Breaking changes should therefore be avoided and an appropriate deprecation cycle
should be followed.

### Stay up to date

The elasticsearch build usually uses the latest Gradle GA release. We stay as close to the
latest Gradle releases as possible. In certain cases an update is blocked by a breaking behaviour
in Gradle. We're usually in contact with the gradle team here or working on a fix
in our build logic to resolve this.

<b>The elasticsearch build fails the build if there's any deprecated gradle api used.</b>

### Make a change in the build

There are a few guidelines to follow that should make your life easier to make changes to the elasticsearch build.
Please add a member of the `es-delivery` team as a reviewer if you're making non trivial changes build.

#### Custom Plugin and Task implementations

Build logic that is used across multiple subprojects should considered to be moved into a Gradle plugin with according Gradle task implmentation.
Elasticsearch specific build logic is located in the `build-tools-internal` subproject including integration tests.

- Gradle plugins and Tasks should be written in Java
- We use a groovy and spock for setting up Gradle integration tests.
  (see https://github.com/elastic/elasticsearch/blob/master/build-tools/src/testFixtures/groovy/org/elasticsearch/gradle/fixtures/AbstractGradleFuncTest.groovy)

#### Declaring tasks

The elasticsearch build makes use of the [task avoidance API](https://docs.gradle.org/current/userguide/task_configuration_avoidance.html) to keep the configuration time of the build low.

- When declaring tasks (in build scripts or custom plugins) this means that we want to _register_ a task like:


  a task should be declared via

  ```
  tasks.register('someTask') {
  }
  ```

  instead of eagerly _creating_ the task
  ```
  task someTask {
  }
  ```

The major difference between these two syntaxes is, that the configuration block of an registered task will only be executed when the task is actually created due to the build requires that task to run. The configuration block of an eagerly created tasks will be executed immediately.

By actually doing less in the gradle configuration time as only creating tasks that are requested as part of the build and by only running the configurations for those requested tasks, using the task avoidance api contributes a major part in keeping our build fast.

#### Adding additional integration tests

Additional integration tests for a certain elasticsearch modules that are specific to certain cluster configuration can be declared in a separate so called `qa` subproject of your module.

The benefit of a dedicated project for these tests are:
- `qa` projects are dedicated two specific usecases and easier to maintain
- It keeps the specific test logic separated from the common test logic.
- You can run those tests in parallel to other projects of the build.

#### Using test fixtures

Sometimes we want to share test fixtures to setup the code under test across multiple projects. There are basically two ways doing so.

Ideally we would use the build-in [java-test-fixtures](https://docs.gradle.org/current/userguide/java_testing.html#sec:java_test_fixtures) gradle plugin.
This plugin relies on having a separate sourceSet for the test fixtures code.

In the elasticsearch codebase we have test fixtures and actual tests within the same sourceSet. Therefore we introduced the `elasticsearch.internal-test-artifact` plugin to provides another build artifact of your project based on the `test` sourceSet.


This artifact can be resolved by the consumer project as shown in the example below:

```
dependencies {
  //add the test fixtures of `:providing-project` to testImplementation configuration.
  testImplementation(testArtifact(project(":fixture-providing-project')))
}
```

This test artifact mechanism makes use of the concept of [component capabilities](https://docs.gradle.org/current/userguide/component_capabilities.html)
similar to how the gradle build-in `java-test-fixtures` plugin works.

`testArtifact` is a shortcut declared in the elasticsearch build. Alternatively you can declare the dependency via

```
dependencies {
  testImplementation(project(":fixture-providing-project')) {
    requireCapabilities("org.elasticsearch.gradle:fixture-providing-project-test-artifacts")
  }
}
```
