Contributing to elasticsearch
=============================

Elasticsearch is a free and open project and we love to receive contributions from our community — you! There are many ways to contribute, from writing tutorials or blog posts, improving the documentation, submitting bug reports and feature requests or writing code which can be incorporated into Elasticsearch itself.

If you want to be rewarded for your contributions, sign up for the [Elastic Contributor Program](https://www.elastic.co/community/contributor). Each time you
make a valid contribution, you’ll earn points that increase your chances of winning prizes and being recognized as a top contributor.

Bug reports
-----------

If you think you have found a bug in Elasticsearch, first make sure that you are testing against the [latest version of Elasticsearch](https://www.elastic.co/downloads/elasticsearch) - your issue may already have been fixed. If not, search our [issues list](https://github.com/elastic/elasticsearch/issues) on GitHub in case a similar issue has already been opened.

It is very helpful if you can prepare a reproduction of the bug. In other words, provide a small test case which we can run to confirm your bug. It makes it easier to find the problem and to fix it. Test cases should be provided as `curl` commands which we can copy and paste into a terminal to run it locally, for example:

```sh
# delete the index
curl -XDELETE localhost:9200/test

# insert a document
curl -XPUT localhost:9200/test/test/1 -d '{
 "title": "test document"
}'

# this should return XXXX but instead returns YYY
curl ....
```

Provide as much information as you can. You may think that the problem lies with your query, when actually it depends on how your data is indexed. The easier it is for us to recreate your problem, the faster it is likely to be fixed.

Feature requests
----------------

If you find yourself wishing for a feature that doesn't exist in Elasticsearch, you are probably not alone. There are bound to be others out there with similar needs. Many of the features that Elasticsearch has today have been added because our users saw the need.
Open an issue on our [issues list](https://github.com/elastic/elasticsearch/issues) on GitHub which describes the feature you would like to see, why you need it, and how it should work.

Contributing code and documentation changes
-------------------------------------------

If you would like to contribute a new feature or a bug fix to Elasticsearch,
please discuss your idea first on the GitHub issue. If there is no GitHub issue
for your idea, please open one. It may be that somebody is already working on
it, or that there are particular complexities that you should know about before
starting the implementation. There are often a number of ways to fix a problem
and it is important to find the right approach before spending time on a PR
that cannot be merged.

We add the `help wanted` label to existing GitHub issues for which community
contributions are particularly welcome, and we use the `good first issue` label
to mark issues that we think will be suitable for new contributors.

The process for contributing to any of the [Elastic repositories](https://github.com/elastic/) is similar. Details for individual projects can be found below.

### Fork and clone the repository

You will need to fork the main Elasticsearch code or documentation repository and clone it to your local machine. See
[github help page](https://help.github.com/articles/fork-a-repo) for help.

Further instructions for specific projects are given below.

### Tips for code changes
Following these tips prior to raising a pull request will speed up the review
cycle.

* Add appropriate unit tests (details on writing tests can be found in the
  [TESTING](TESTING.asciidoc) file)
* Add integration tests, if applicable
* Make sure the code you add follows the [formatting guidelines](#java-language-formatting-guidelines)
* Lines that are not part of your change should not be edited (e.g. don't format
  unchanged lines, don't reorder existing imports)
* Add the appropriate [license headers](#license-headers) to any new files

### Submitting your changes

Once your changes and tests are ready to submit for review:

1. Test your changes

    Run the test suite to make sure that nothing is broken. See the
[TESTING](TESTING.asciidoc) file for help running tests.

2. Sign the Contributor License Agreement

    Please make sure you have signed our [Contributor License Agreement](https://www.elastic.co/contributor-agreement/). We are not asking you to assign copyright to us, but to give us the right to distribute your code without restriction. We ask this of all contributors in order to assure our users of the origin and continuing existence of the code. You only need to sign the CLA once.

3. Rebase your changes

    Update your local repository with the most recent code from the main Elasticsearch repository, and rebase your branch on top of the latest master branch. We prefer your initial changes to be squashed into a single commit. Later, if we ask you to make changes, add them as separate commits.  This makes them easier to review.  As a final step before merging we will either ask you to squash all commits yourself or we'll do it for you.


4. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".

Then sit back and wait. There will probably be discussion about the pull request and, if any changes are needed, we would love to work with you to get your pull request merged into Elasticsearch.

Please adhere to the general guideline that you should never force push
to a publicly shared branch. Once you have opened your pull request, you
should consider your branch publicly shared. Instead of force pushing
you can just add incremental commits; this is generally easier on your
reviewers. If you need to pick up changes from master, you can merge
master into your branch. A reviewer might ask you to rebase a
long-running pull request in which case force pushing is okay for that
request. Note that squashing at the end of the review process should
also not be done, that can be done when the pull request is [integrated
via GitHub](https://github.com/blog/2141-squash-your-commits).

Contributing to the Elasticsearch codebase
------------------------------------------

**Repository:** [https://github.com/elastic/elasticsearch](https://github.com/elastic/elasticsearch)

JDK 15 is required to build Elasticsearch. You must have a JDK 15 installation
with the environment variable `JAVA_HOME` referencing the path to Java home for
your JDK 15 installation. By default, tests use the same runtime as `JAVA_HOME`.
However, since Elasticsearch supports JDK 8, the build supports compiling with
JDK 15 and testing on a JDK 8 runtime; to do this, set `RUNTIME_JAVA_HOME`
pointing to the Java home of a JDK 8 installation. Note that this mechanism can
be used to test against other JDKs as well, this is not only limited to JDK 8.

> Note: It is also required to have `JAVA8_HOME`, `JAVA9_HOME`, `JAVA10_HOME`
and `JAVA11_HOME`, `JAVA12_HOME`, `JAVA13_HOME`, `JAVA14_HOME`, and `JAVA15_HOME`
available so that the tests can pass.

Elasticsearch uses the Gradle wrapper for its build. You can execute Gradle
using the wrapper via the `gradlew` script on Unix systems or `gradlew.bat`
script on Windows in the root of the repository. The examples below show the
usage on Unix.

We support development in IntelliJ versions IntelliJ 2020.1 and
onwards and Eclipse 2020-3 and onwards.

[Docker](https://docs.docker.com/install/) is required for building some Elasticsearch artifacts and executing certain test suites. You can run Elasticsearch without building all the artifacts with:

    ./gradlew :run

That'll spend a while building Elasticsearch and then it'll start Elasticsearch,
writing its log above Gradle's status message. We log a lot of stuff on startup,
specifically these lines tell you that Elasticsearch is ready:

    [2020-05-29T14:50:35,167][INFO ][o.e.h.AbstractHttpServerTransport] [runTask-0] publish_address {127.0.0.1:9200}, bound_addresses {[::1]:9200}, {127.0.0.1:9200}
    [2020-05-29T14:50:35,169][INFO ][o.e.n.Node               ] [runTask-0] started

But to be honest its typically easier to wait until the console stops scrolling
and then run `curl` in another window like this:

    curl -u elastic:password localhost:9200


### Importing the project into IntelliJ IDEA

The minimum IntelliJ IDEA version required to import the Elasticsearch project is 2020.1
Elasticsearch builds using Java 15. When importing into IntelliJ you will need
to define an appropriate SDK. The convention is that **this SDK should be named
"15"** so that the project import will detect it automatically. For more details
on defining an SDK in IntelliJ please refer to [their documentation](https://www.jetbrains.com/help/idea/sdk.html#define-sdk).
SDK definitions are global, so you can add the JDK from any project, or after
project import. Importing with a missing JDK will still work, IntelliJ will
simply report a problem and will refuse to build until resolved.

You can import the Elasticsearch project into IntelliJ IDEA via:

 - Select **File > Open**
 - In the subsequent dialog navigate to the root `build.gradle` file
 - In the subsequent dialog select **Open as Project**

#### Checkstyle

If you have the [Checkstyle] plugin installed, you can configure IntelliJ to
check the Elasticsearch code. However, the Checkstyle configuration file does
not work by default with the IntelliJ plugin, so instead an IDE-specific config
file is generated automatically after IntelliJ finishes syncing. You can
manually generate the file with `./gradlew configureIdeCheckstyle` in case
it is removed due to a `./gradlew clean` or other action.

IntelliJ should be automatically configured to use the generated rules after
import via the `.idea/checkstyle-idea.xml` configuration file. No further
action is required.

#### Formatting

We are in the process of migrating towards automatic formatting Java file
using [spotless], backed by the Eclipse formatter. **We strongly recommend
installing using the [Eclipse Code Formatter] plugin** so that you can
apply the correct formatting directly in IntelliJ.  The configuration for
the plugin is held in `.idea/eclipseCodeFormatter.xml` and should be
automatically applied, but manual instructions are below in case you you
need them.

   1. Open **Preferences > Other Settings > Eclipse Code Formatter**
   2. Click "Use the Eclipse Code Formatter"
   3. Under "Eclipse formatter config", select "Eclipse workspace/project
      folder or config file"
   4. Click "Browse", and navigate to the file `buildSrc/formatterConfig.xml`
   5. **IMPORTANT** - make sure "Optimize Imports" is **NOT** selected.
   6. Click "OK"

Note that only some sub-projects in the Elasticsearch project are currently
fully-formatted. You can see a list of project that **are not**
automatically formatted in
[build-tools-internal/src/main/groovy/elasticsearch.formatting.gradle](build-tools-internal/src/main/groovy/elasticsearch.formatting.gradle).

### Importing the project into Eclipse

Elasticsearch builds using Gradle and Java 15. When importing into Eclipse you
will either need to use an appropriate JDK to run Eclipse itself (e.g. by
specifying the VM in [eclipse.ini](https://wiki.eclipse.org/Eclipse.ini) or by
defining the JDK Gradle uses by setting **Preferences** > **Gradle** >
**Advanced Options** > **Java home** to an appropriate version.

IMPORTANT: If you have previously imported the project by running `./gradlew eclipse`
           then you must build an entirely new workspace and `git clean -xdf` to
           blow away *everything* that the gradle eclipse plugin made.

 - Select **File > Import...**
 - Select **Existing Gradle Project**
 - Select **Next** then **Next** again
 - Set the **Project root directory** to the root of your elasticsearch clone
 - Click **Finish**

This will spin for a long, long time but you'll see many errors about circular
dependencies. Fix them:

 - Select **Window > Preferences**
 - Select **Java > Compiler > Building**
 - Look under **Build Path Problems**
 - Set **Circular dependencies** to **Warning**
 - Apply that and let the build spin away for a while

Next you'll want to import our auto-formatter:

 - Select **Window > Preferences**
 - Select **Java > Code Style > Formatter**
 - Click **Import**
 - Import the file at **buildSrc/formatterConfig.xml**
 - Make sure it is the **Active profile**

Finally, set up import order:

 - Select **Window > Preferences**
 - Select **Java > Code Style > Organize Imports**
 - Click **Import...**
 - Import the file at **buildSrc/elastic.importorder**
 - Set the **Number of imports needed for `.*`** to ***9999***
 - Set the **Number of static imports needed for `.*`** to ***9999*** as well
 - Apply that

IMPORTANT: There is an option in **Gradle** for **Automatic Project Synchronization**.
           As convenient as it'd be for the projects to always be perfect this
           tends to add many many seconds to every branch change. Instead, you
           should manually right click on a project and
           **Gradle > Refresh Gradle Project** if the configuration is out of
           date.

As we add more subprojects you might have to re-import the gradle project (the
first step) again. There is no need to blow away the existing projects before
doing that.

### REST Endpoint Conventions

Elasticsearch typically uses singular nouns rather than plurals in URLs.
For example:

    /_ingest/pipeline
    /_ingest/pipeline/{id}

but not:

    /_ingest/pipelines
    /_ingest/pipelines/{id}

You may find counterexamples, but new endpoints should use the singular
form.

### Java Language Formatting Guidelines

Java files in the Elasticsearch codebase are automatically formatted using
the [Spotless Gradle] plugin. All new projects are automatically formatted,
while existing projects are gradually being opted-in. The formatting check
can be run explicitly with:

    ./gradlew spotlessJavaCheck

The code can be formatted with:

    ./gradlew spotlessApply

These tasks can also be run for specific subprojects, e.g.

    ./gradlew server:spotlessJavaCheck

Please follow these formatting guidelines:

* Java indent is 4 spaces
* Line width is 140 characters
* Lines of code surrounded by `// tag::NAME` and `// end::NAME` comments are included
  in the documentation and should only be 76 characters wide not counting
  leading indentation. Such regions of code are not formatted automatically as
  it is not possible to change the line length rule of the formatter for
  part of a file. Please format such sections sympathetically with the rest
  of the code, while keeping lines to maximum length of 76 characters.
* Wildcard imports (`import foo.bar.baz.*`) are forbidden and will cause
  the build to fail.
* If *absolutely* necessary, you can disable formatting for regions of code
  with the `// tag::NAME` and `// end::NAME` directives, but note that
  these are intended for use in documentation, so please make it clear what
  you have done, and only do this where the benefit clearly outweighs the
  decrease in consistency.
* Note that Javadoc and block comments i.e. `/* ... */` are not formatted,
  but line comments i.e `// ...` are.
* Negative boolean expressions must use the form `foo == false` instead of
  `!foo` for better readability of the code. This is enforced via
  Checkstyle. Conversely, you should not write e.g. `if (foo == true)`, but
  just `if (foo)`.

#### Editor / IDE Support

IntelliJ IDEs can
[import](https://blog.jetbrains.com/idea/2014/01/intellij-idea-13-importing-code-formatter-settings-from-eclipse/)
the same settings file, and / or use the [Eclipse Code Formatter] plugin.

You can also tell Spotless to [format a specific
file](https://github.com/diffplug/spotless/tree/master/plugin-gradle#can-i-apply-spotless-to-specific-files)
from the command line.

### Javadoc

Good Javadoc can help with navigating and understanding code. Elasticsearch
has some guidelines around when to write Javadoc and when not to, but note
that we don't want to be overly prescriptive. The intent of these guidelines
is to be helpful, not to turn writing code into a chore.

#### The short version

   1. Always add Javadoc to new code.
   2. Add Javadoc to existing code if you can.
   3. Document the "why", not the "how", unless that's important to the
      "why".
   4. Don't document anything trivial or obvious (e.g. getters and
      setters). In other words, the Javadoc should add some value.

#### The long version

   1. If you add a new Java package, please also add package-level
      Javadoc that explains what the package is for. This can just be a
      reference to a more foundational / parent package if appropriate. An
      example would be a package hierarchy for a new feature or plugin -
      the package docs could explain the purpose of the feature, any
      caveats, and possibly some examples of configuration and usage.
   2. New classes and interfaces must have class-level Javadoc that
      describes their purpose. There are a lot of classes in the
      Elasticsearch repository, and it's easier to navigate when you
      can quickly find out what is the purpose of a class. This doesn't
      apply to inner classes or interfaces, unless you expect them to be
      explicitly used outside their parent class.
   3. New public methods must have Javadoc, because they form part of the
      contract between the class and its consumers. Similarly, new abstract
      methods must have Javadoc because they are part of the contract
      between a class and its subclasses. It's important that contributors
      know why they need to implement a method, and the Javadoc should make
      this clear. You don't need to document a method if it's overriding an
      abstract method (either from an abstract superclass or an interface),
      unless your implementation is doing something "unexpected" e.g. deviating
      from the intent of the original method.
   4. Following on from the above point, please add docs to existing public
      methods if you are editing them, or to abstract methods if you can.
   5. Non-public, non-abstract methods don't require Javadoc, but if you feel
      that adding some would make it easier for other developers to
      understand the code, or why it's written in a particular way, then please
      do so.
   6. Properties don't need to have Javadoc, but please add some if there's
      something useful to say.
   7. Javadoc should not go into low-level implementation details unless
      this is critical to understanding the code e.g. documenting the
      subtleties of the implementation of a private method. The point here
      is that implementations will change over time, and the Javadoc is
      less likely to become out-of-date if it only talks about the what is
      the purpose of the code, not what it does.
   8. Examples in Javadoc can be very useful, so feel free to add some if
      you can reasonably do so i.e. if it takes a whole page of code to set
      up an example, then Javadoc probably isn't the right place for it.
      Longer or more elaborate examples are probably better suited
      to the package docs.
   9. Test methods are a good place to add Javadoc, because you can use it
      to succinctly describe e.g. preconditions, actions and expectations
      of the test, more easily that just using the test name alone. Please
      consider documenting your tests in this way.
   10. Sometimes you shouldn't add Javadoc:
       1. Where it adds no value, for example where a method's
          implementation is trivial such as with getters and setters, or a
          method just delegates to another object.
       2. However, you should still add Javadoc if there are caveats around
          calling a method that are not immediately obvious from reading the
          method's implementation in isolation.
       3. You can omit Javadoc for simple classes, e.g. where they are a
          simple container for some data. However, please consider whether a
          reader might still benefit from some additional background, for
          example about why the class exists at all.
   11. Not all comments need to be Javadoc. Sometimes it will make more
       sense to add comments in a method's body, for example due to important
       implementation decisions or "gotchas". As a general guide, if some
       information forms part of the contract between a method and its callers,
       then it should go in the Javadoc, otherwise you might consider using
       regular comments in the code. Remember as well that Elasticsearch
       has extensive [user documentation](./docs), and it is not the role
       of Javadoc to replace that.
        * If a method's performance is "unexpected" then it's good to call that
           out in the Javadoc. This is especially helpful if the method is usually fast but sometimes
           very slow (shakes fist at caching).
   12. Please still try to make class, method or variable names as
       descriptive and concise as possible, as opposed to relying solely on
       Javadoc to describe something.
   13. Use `@link` to add references to related resources in the codebase. Or
       outside the code base.
       1. `@see` is much more limited than `@link`. You can use it but most of
          the time `@link` flows better.
   14. If you need help writing Javadoc, just ask!

Finally, use your judgement! Base your decisions on what will help other
developers - including yourself, when you come back to some code
3 months in the future, having forgotten how it works.

### License Headers

We require license headers on all Java files. With the exception of the
top-level `x-pack` directory, all contributed code should have the following
license header unless instructed otherwise:

    /*
     * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
     * or more contributor license agreements. Licensed under the Elastic License
     * 2.0 and the Server Side Public License, v 1; you may not use this file except
     * in compliance with, at your election, the Elastic License 2.0 or the Server
     * Side Public License, v 1.
     */

The top-level `x-pack` directory contains code covered by the [Elastic
license](licenses/ELASTIC-LICENSE-2.0.txt). Community contributions to this code are
welcome, and should have the following license header unless instructed
otherwise:

    /*
     * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
     * or more contributor license agreements. Licensed under the Elastic License
     * 2.0; you may not use this file except in compliance with the Elastic License
     * 2.0.
     */

It is important that the only code covered by the Elastic licence is contained
within the top-level `x-pack` directory. The build will fail its pre-commit
checks if contributed code does not have the appropriate license headers.

> **NOTE:** If you have imported the project into IntelliJ IDEA the project will
> be automatically configured to add the correct license header to new source
> files based on the source location.

### Type-checking, generics and casting

You should try to write code that does not require suppressing any warnings from
the compiler, e.g. suppressing type-checking, raw generics, and so on. However,
this isn't always possible or practical. In such cases, you should use the
`@SuppressWarnings` annotations to silence the compiler warning, trying to keep
the scope of the suppression as small as possible. Where a piece of code
requires a lot of suppressions, it may be better to apply a single suppression
at a higher level e.g. at the method or even class level. Use your judgement.

There are also cases where the compiler simply refuses to accept an assignment
or cast of any kind, because it lacks the information to know that the types are
OK. In such cases, you can use
the [`Types.forciblyCast`](libs/core/src/main/java/org/elasticsearch/core/Types.java)
utility method. As the name suggests, you can coerce any type to any other type,
so please use it as a last resort.

### Creating A Distribution

Run all build commands from within the root directory:

    cd elasticsearch/

To build a darwin-tar distribution, run this command:

    ./gradlew -p distribution/archives/darwin-tar assemble

You will find the distribution under:

    ./distribution/archives/darwin-tar/build/distributions/

To create all build artifacts (e.g., plugins and Javadocs) as well as
distributions in all formats, run this command:

    ./gradlew assemble

> **NOTE:** Running the task above will fail if you don't have an available
> Docker installation.

The package distributions (Debian and RPM) can be found under:

    ./distribution/packages/(deb|rpm|oss-deb|oss-rpm)/build/distributions/

The archive distributions (tar and zip) can be found under:

    ./distribution/archives/(darwin-tar|linux-tar|windows-zip|oss-darwin-tar|oss-linux-tar|oss-windows-zip)/build/distributions/

### Running The Full Test Suite

Before submitting your changes, run the test suite to make sure that nothing is broken, with:

    ./gradlew check

If your changes affect only the documentation, run:

    ./gradlew -p docs check

For more information about testing code examples in the documentation, see
https://github.com/elastic/elasticsearch/blob/master/docs/README.asciidoc

### Project layout

This repository is split into many top level directories. The most important
ones are:

#### `docs`
Documentation for the project.

#### `distribution`
Builds our tar and zip archives and our rpm and deb packages.

#### `libs`
Libraries used to build other parts of the project. These are meant to be
internal rather than general purpose. We have no plans to
[semver](https://semver.org/) their APIs or accept feature requests for them.
We publish them to maven central because they are dependencies of our plugin
test framework, high level rest client, and jdbc driver but they really aren't
general purpose enough to *belong* in maven central. We're still working out
what to do here.

#### `modules`
Features that are shipped with Elasticsearch by default but are not built in to
the server. We typically separate features from the server because they require
permissions that we don't believe *all* of Elasticsearch should have or because
they depend on libraries that we don't believe *all* of Elasticsearch should
depend on.

For example, reindex requires the `connect` permission so it can perform
reindex-from-remote but we don't believe that the *all* of Elasticsearch should
have the "connect". For another example, Painless is implemented using antlr4
and asm and we don't believe that *all* of Elasticsearch should have access to
them.

#### `plugins`
Officially supported plugins to Elasticsearch. We decide that a feature should
be a plugin rather than shipped as a module because we feel that it is only
important to a subset of users, especially if it requires extra dependencies.

The canonical example of this is the ICU analysis plugin. It is important for
folks who want the fairly language neutral ICU analyzer but the library to
implement the analyzer is 11MB so we don't ship it with Elasticsearch by
default.

Another example is the `discovery-gce` plugin. It is *vital* to folks running
in [GCP](https://cloud.google.com/) but useless otherwise and it depends on a
dozen extra jars.

#### `qa`
Honestly this is kind of in flux and we're not 100% sure where we'll end up.
Right now the directory contains
* Tests that require multiple modules or plugins to work
* Tests that form a cluster made up of multiple versions of Elasticsearch like
full cluster restart, rolling restarts, and mixed version tests
* Tests that test the Elasticsearch clients in "interesting" places like the
`wildfly` project.
* Tests that test Elasticsearch in funny configurations like with ingest
disabled
* Tests that need to do strange things like install plugins that thrown
uncaught `Throwable`s or add a shutdown hook
But we're not convinced that all of these things *belong* in the qa directory.
We're fairly sure that tests that require multiple modules or plugins to work
should just pick a "home" plugin. We're fairly sure that the multi-version
tests *do* belong in qa. Beyond that, we're not sure. If you want to add a new
qa project, open a PR and be ready to discuss options.

#### `server`
The server component of Elasticsearch that contains all of the modules and
plugins. Right now things like the high level rest client depend on the server
but we'd like to fix that in the future.

#### `test`
Our test framework and test fixtures. We use the test framework for testing the
server, the plugins, and modules, and pretty much everything else. We publish
the test framework so folks who develop Elasticsearch plugins can use it to
test the plugins. The test fixtures are external processes that we start before
running specific tests that rely on them.

For example, we have an hdfs test that uses mini-hdfs to test our
repository-hdfs plugin.

#### `x-pack`
Commercially licensed code that integrates with the rest of Elasticsearch. The
`docs` subdirectory functions just like the top level `docs` subdirectory and
the `qa` subdirectory functions just like the top level `qa` subdirectory. The
`plugin` subdirectory contains the x-pack module which runs inside the
Elasticsearch process.

### Gradle Build

We use Gradle to build Elasticsearch because it is flexible enough to not only
build and package Elasticsearch, but also orchestrate all of the ways that we
have to test Elasticsearch.

#### Configurations

Gradle organizes dependencies and build artifacts into "configurations" and
allows you to use these configurations arbitrarily. Here are some of the most
common configurations in our build and how we use them:

<dl>
<dt>`implementation`</dt><dd>Dependencies that are used by the project
at compile and runtime but are not exposed as a compile dependency to other dependent projects.
Dependencies added to the `implementation` configuration are considered an implementation detail
that can be changed at a later date without affecting any dependent projects.</dd>
<dt>`api`</dt><dd>Dependencies that are used as compile and runtime dependencies of a project
 and are considered part of the external api of the project.
<dt>`runtimeOnly`</dt><dd>Dependencies that not on the classpath at compile time but
are on the classpath at runtime. We mostly use this configuration to make sure that
we do not accidentally compile against dependencies of our dependencies also
known as "transitive" dependencies".</dd>
<dt>`compileOnly`</dt><dd>Code that is on the classpath at compile time but that
should not be shipped with the project because it is "provided" by the runtime
somehow. Elasticsearch plugins use this configuration to include dependencies
that are bundled with Elasticsearch's server.</dd>
<dt>`testImplementation`</dt><dd>Code that is on the classpath for compiling tests
that are part of this project but not production code. The canonical example
of this is `junit`.</dd>
</dl>


Reviewing and accepting your contribution
-----------------------------------------

We review every contribution carefully to ensure that the change is of high
quality and fits well with the rest of the Elasticsearch codebase. If accepted,
we will merge your change and usually take care of backporting it to
appropriate branches ourselves.

We really appreciate everyone who is interested in contributing to
Elasticsearch and regret that we sometimes have to reject contributions even
when they might appear to make genuine improvements to the system. Reviewing
contributions can be a very time-consuming task, yet the team is small and our
time is very limited. In some cases the time we would need to spend on reviews
would outweigh the benefits of a change by preventing us from working on other
more beneficial changes instead.

Please discuss your change in a Github issue before spending much time on its
implementation. We sometimes have to reject contributions that duplicate other
efforts, take the wrong approach to solving a problem, or solve a problem which
does not need solving. An up-front discussion often saves a good deal of wasted
time in these cases.

We normally immediately reject isolated PRs that only perform simple
refactorings or otherwise "tidy up" certain aspects of the code. We think the
benefits of this kind of change are very small, and in our experience it is not
worth investing the substantial effort needed to review them. This especially
includes changes suggested by tools.

We sometimes reject contributions due to the low quality of the submission
since low-quality submissions tend to take unreasonable effort to review
properly. Quality is rather subjective so it is hard to describe exactly how to
avoid this, but there are some basic steps you can take to reduce the chances
of rejection. Follow the guidelines listed above when preparing your changes.
You should add tests that correspond with your changes, and your PR should pass
affected test suites too. It makes it much easier to review if your code is
formatted correctly and does not include unnecessary extra changes.

We sometimes reject contributions if we find ourselves performing many review
iterations without making enough progress. Some iteration is expected,
particularly on technically complicated changes, and there's no fixed limit on
the acceptable number of review cycles since it depends so much on the nature
of the change. You can help to reduce the number of iterations by reviewing
your contribution yourself or in your own team before asking us for a review.
You may be surprised how many comments you can anticipate and address by taking
a short break and then carefully looking over your changes again.

We expect you to follow up on review comments somewhat promptly, but recognise
that everyone has many priorities for their time and may not be able to respond
for several days. We will understand if you find yourself without the time to
complete your contribution, but please let us know that you have stopped
working on it. We will try to send you a reminder if we haven't heard from you
in a while, but may end up closing your PR if you do not respond for too long.

If your contribution is rejected we will close the pull request with a comment
explaining why. This decision isn't always final: if you feel we have
misunderstood your intended change or otherwise think that we should reconsider
then please continue the conversation with a comment on the pull request and
we'll do our best to address any further points you raise.

Contributing as part of a class
-------------------------------
In general Elasticsearch is happy to accept contributions that were created as
part of a class but strongly advise against making the contribution as part of
the class. So if you have code you wrote for a class feel free to submit it.

Please, please, please do not assign contributing to Elasticsearch as part of a
class. If you really want to assign writing code for Elasticsearch as an
assignment then the code contributions should be made to your private clone and
opening PRs against the primary Elasticsearch clone must be optional, fully
voluntary, not for a grade, and without any deadlines.

Because:

* While the code review process is likely very educational, it can take wildly
varying amounts of time depending on who is available, where the change is, and
how deep the change is. There is no way to predict how long it will take unless
we rush.
* We do not rush reviews without a very, very good reason. Class deadlines
aren't a good enough reason for us to rush reviews.
* We deeply discourage opening a PR you don't intend to work through the entire
code review process because it wastes our time.
* We don't have the capacity to absorb an entire class full of new contributors,
especially when they are unlikely to become long time contributors.

Finally, we require that you run `./gradlew check` before submitting a
non-documentation contribution. This is mentioned above, but it is worth
repeating in this section because it has come up in this context.

[intellij]: https://blog.jetbrains.com/idea/2017/07/intellij-idea-2017-2-is-here-smart-sleek-and-snappy/
[Checkstyle]: https://plugins.jetbrains.com/plugin/1065-checkstyle-idea
[spotless]: https://github.com/diffplug/spotless
[Eclipse Code Formatter]: https://plugins.jetbrains.com/plugin/6546-eclipse-code-formatter
[Spotless Gradle]: https://github.com/diffplug/spotless/tree/master/plugin-gradle
