/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Functions that take a row of data and produce a row of data without holding
 * any state between rows. This includes both the {@link org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction}
 * subclass to link into the QL infrastucture and the {@link org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator}
 * implementation to run the actual function.
 *
 * <h2>Guide to adding new function</h2>
 * <p>
 *     Adding functions is <strong>fairly</strong> easy and should be fun!
 *     This is a step by step list of how to do it.
 * </p>
 * <ol>
 *     <li>Fork the <a href="github.com/elastic/elasticsearch">Elasticsearch repo</a>.</li>
 *     <li>Clone your fork locally.</li>
 *     <li>Add Elastic's remote, it should look a little like:
 *         <pre>{@code
 * [remote "elastic"]
 * url = git@github.com:elastic/elasticsearch.git
 * fetch = +refs/heads/*:refs/remotes/elastic/*
 * [remote "nik9000"]
 * url = git@github.com:nik9000/elasticsearch.git
 * fetch = +refs/heads/*:refs/remotes/nik9000/*
 *         }</pre>
 *     </li>
 *     <li>
 *         Feel free to use {@code git} as a scratch pad. We're going to squash all commits
 *         before merging and will only keep the PR subject line and description in the
 *         commit message.
 *     </li>
 *     <li>
 *         Open Elasticsearch in IntelliJ.
 *     </li>
 *     <li>
 *         Run the csv tests (see {@code x-pack/plugin/esql/src/test/java/org/elasticsearch/xpack/esql/CsvTests.java})
 *         from within Intellij or, alternatively, via Gradle:
 *         {@code ./gradlew :x-pack:plugin:esql:test --tests "org.elasticsearch.xpack.esql.CsvTests"}
 *         IntelliJ will take a few minutes to compile everything but the test itself should take only a few seconds.
 *         This is a fast path to running ESQL's integration tests.
 *     </li>
 *     <li>
 *         Pick one of the csv-spec files in {@code x-pack/plugin/esql/qa/testFixtures/src/main/resources/}
 *         and add a test for the function you want to write. These files are roughly themed but there
 *         isn't a strong guiding principle in the theme.
 *     </li>
 *     <li>
 *         Rerun the {@code CsvTests} and watch your new test fail. Yay, TDD doing it's job.
 *     </li>
 *     <li>
 *         Find a function in this package similar to the one you are working on and copy it to build
 *         yours. There's some ceremony required in each function class to make it constant foldable
 *         and return the right types. Take a stab at these, but don't worry too much about getting
 *         it right.
 *     </li>
 *     <li>
 *         There are also methods annotated with {@link org.elasticsearch.compute.ann.Evaluator}
 *         that contain the actual inner implementation of the function. Modify those to look right
 *         and click {@code Build->Recompile 'FunctionName.java'} in IntelliJ or run the
 *         {@code CsvTests} again. This should generate an
 *         {@link org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator} implementation
 *         calling the method annotated with {@link org.elasticsearch.compute.ann.Evaluator}. Please commit the
 *         generated evaluator before submitting your PR.
 *     <li>
 *         Once your evaluator is generated you can implement
 *         {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper#toEvaluator},
 *         having it return the generated evaluator.
 *     </li>
 *     <li>
 *         Add your function to {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 *         This links it into the language and {@code SHOW FUNCTIONS}. Also add your function to
 *         {@link org.elasticsearch.xpack.esql.io.stream.PlanNamedTypes}. This makes your function
 *         serializable over the wire. Mostly you can copy existing implementations for both.
 *     </li>
 *     <li>
 *         Rerun the {@code CsvTests}. They should find your function and maybe even pass. Add a
 *         few more tests in the csv-spec tests. They run quickly so it isn't a big deal having
 *         half a dozen of them per function. In fact, it's useful to add more complex combinations
 *         of things here, just to catch any accidental strange interactions. For example, it is
 *         probably a good idea to have your function passes as a parameter to another function
 *         like {@code EVAL foo=MOST(0, MY_FUNCTION(emp_no))}. And likely useful to try the reverse
 *         like {@code EVAL foo=MY_FUNCTION(MOST(languages + 10000, emp_no)}.
 *     </li>
 *     <li>
 *         Now it's time to make a unit test! The infrastructure for these is under some flux at
 *         the moment, but it's good to extend from {@code AbstractScalarFunctionTestCase}. All of
 *         these tests are parameterized and expect to spend some time finding good parameters.
 *     </li>
 *     <li>
 *         Once you are happy with the tests run the auto formatter:
 *         {@code ./gradlew -p x-pack/plugin/esql/ spotlessApply}
 *     </li>
 *     <li>
 *         Now you can run all of the ESQL tests like CI:
 *         {@code ./gradlew -p x-pack/plugin/esql/ check}
 *     </li>
 *     <li>
 *         Now it's time to write some docs! Open {@code docs/reference/esql/esql-functions.asciidoc}
 *         and add your function in alphabetical order to the list at the top and then add it to
 *         the includes below.
 *     </li>
 *     <li>
 *         Now go make a file to include. You can start by copying one of it's neighbors.
 *     </li>
 *     <li>
 *         It's important that any examples you add to the docs be included from the csv-spec file.
 *         That looks like:
 *         <pre>{@code
 * [source.merge.styled,esql]
 * ----
 * include::{esql-specs}/math.csv-spec[tag=mv_min]
 * ----
 * [%header.monospaced.styled,format=dsv,separator=|]
 * |===
 * include::{esql-specs}/math.csv-spec[tag=mv_min-result]
 * |===
 *         }</pre>
 *         This includes the bit of the csv-spec file fenced by {@code // tag::mv_min[]}. You'll
 *         want a fence descriptive for your function. Consider the non-includes lines to be
 *         asciidoc ceremony to make the result look right in the rendered docs.
 *     </li>
 *     <li>
 *         Generate a syntax diagram and a table with supported types by running the tests via
 *         gradle: {@code ./gradlew x-pack:plugin:esql:test}
 *         The generated files can be found here
 *         {@code docs/reference/esql/functions/signature/myfunction.svg }
 *         and here
 *         {@code docs/reference/esql/functions/types/myfunction.asciidoc}
 *         Make sure to commit them and reference them in your doc file. There are plenty of examples on how
 *         to reference those files e.g. {@code docs/reference/esql/functions/sin.asciidoc}.
 *     </li>
 *     <li>
 *          Build the docs by cloning the <a href="https://github.com/elastic/docs">docs repo</a>
 *          and running:
 *          <pre>{@code
 * ../docs/build_docs --doc docs/reference/index.asciidoc --open --chunk 1
 *          }</pre>
 *          from the elasticsearch directory. The first time you run the docs build it does a bunch
 *          of things with docker to get itself ready. Hopefully you can sit back and watch the show.
 *          It won't need to do it a second time unless some poor soul updates the Dockerfile in the
 *          docs repo.
 *     </li>
 *     <li>
 *         When it finishes building it'll open a browser window. Go to the
 *         <a href="http://localhost:8000/guide/esql-functions.html">functions page</a> to see your
 *         function in the list and follow it's link to get to the page you built. Make sure it
 *         looks ok.
 *     </li>
 *     <li>
 *         Open the PR. The subject and description of the PR are important because those'll turn
 *         into the commit message we see in the commit history. Good PR descriptions make me very
 *         happy. But functions don't need an essay.
 *     </li>
 *     <li>
 *         Add the {@code >enhancement} and {@code :Query Languages/ES|QL} tags if you are able.
 *         Request a review if you can, probably from one of the folks that github proposes to you.
 *     </li>
 *     <li>
 *         CI might fail for random looking reasons. The first thing you should do is merge {@code main}
 *         into your PR branch. That's usually just:
 *         <pre>{@code
 * git checkout main && git pull elastic main && git checkout mybranch && git merge main
 *         }</pre>
 *         Don't worry about the commit message. It'll get squashed away in the merge.
 *     </li>
 * </ol>
 */
package org.elasticsearch.xpack.esql.expression.function.scalar;
