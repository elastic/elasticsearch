/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Functions that take a row of data and produce a row of data without holding
 * any state between rows. This includes both the {@link org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction}
 * subclass to link into the ESQL core infrastructure and the {@link org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator}
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
 *     <li>Add Elastic’s remote, it should look a little like:
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
 *         This is a fast path to running ESQL’s integration tests.
 *     </li>
 *     <li>
 *         Pick one of the csv-spec files in {@code x-pack/plugin/esql/qa/testFixtures/src/main/resources/}
 *         and add a test for the function you want to write. These files are roughly themed but there
 *         isn’t a strong guiding principle in the organization.
 *     </li>
 *     <li>
 *         Rerun the {@code CsvTests} and watch your new test fail. Yay, TDD doing it’s job.
 *     </li>
 *     <li>
 *         Find a function in this package similar to the one you are working on and copy it to build
 *         yours. There’s some ceremony required in each function class to make it constant foldable,
 *         and return the right types. Take a stab at these, but don’t worry too much about getting
 *         it right. Your function might extend from one of several abstract base classes, all of
 *         those are fine for this guide, but might have special instructions called out later.
 *         Known good base classes:
 *         <ul>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction}</li>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction}</li>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.UnaryScalarFunction}
 *                 or any subclass like {@code AbstractTrigonometricFunction}</li>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction}</li>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.math.DoubleConstantFunction}</li>
 *         </ul>
 *     </li>
 *     <li>
 *         There are also methods annotated with {@link org.elasticsearch.compute.ann.Evaluator}
 *         that contain the actual inner implementation of the function. They are usually named
 *         "process" or "processInts" or "processBar". Modify those to look right and run the {@code CsvTests}
 *         again. This should generate an {@link org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator}
 *         implementation calling the method annotated with {@link org.elasticsearch.compute.ann.Evaluator}.
 *.        To make it work with IntelliJ, also click {@code Build->Recompile 'FunctionName.java'}.
 *         Please commit the generated evaluator before submitting your PR.
 *         <p>
 *             NOTE: The function you copied may have a method annotated with
 *             {@link org.elasticsearch.compute.ann.ConvertEvaluator} or
 *             {@link org.elasticsearch.compute.ann.MvEvaluator} instead of
 *             {@link org.elasticsearch.compute.ann.Evaluator}. Those do similar things and the
 *             instructions should still work for you regardless. If your function contains an implementation
 *             of {@link org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator} written by
 *             hand then please stop and ask for help. This is not a good first function.
 *         </p>
 *         <p>
 *             NOTE 2: Regardless of which annotation is on your "process" method you can learn more
 *             about the options for generating code from the javadocs on those annotations.
 *         </p>
 *     <li>
 *         Once your evaluator is generated you can have your function return it,
 *         generally by implementing {@link org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper#toEvaluator}.
 *         It’s possible that your abstract base class implements that function and
 *         will need you to implement something else:
 *         <ul>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction}: {@code factories}</li>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.multivalue.AbstractMultivalueFunction}:
 *                 {@code evaluator}</li>
 *             <li>{@code AbstractTrigonometricFunction}: {@code doubleEvaluator}</li>
 *             <li>{@link org.elasticsearch.xpack.esql.expression.function.scalar.math.DoubleConstantFunction}: nothing!</li>
 *         </ul>
 *     </li>
 *     <li>
 *         Add your function to {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 *         This links it into the language and {@code META FUNCTIONS}.
 *     </li>
 *     <li>
 *         Implement serialization for your function by implementing
 *         {@link org.elasticsearch.common.io.stream.NamedWriteable#getWriteableName},
 *         {@link org.elasticsearch.common.io.stream.NamedWriteable#writeTo},
 *         and a deserializing constructor. Then add an {@link org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry}
 *         constant and register it. To register it, look for a method like
 *         {@link org.elasticsearch.xpack.esql.expression.function.scalar.ScalarFunctionWritables#getNamedWriteables()}
 *         in your function’s class hierarchy. Keep going up until you hit a function with that name.
 *         Then add your new "ENTRY" constant to the list it returns.
 *     </li>
 *     <li>
 *         Rerun the {@code CsvTests}. They should find your function and maybe even pass. Add a
 *         few more tests in the csv-spec tests. They run quickly so it isn’t a big deal having
 *         half a dozen of them per function. In fact, it’s useful to add more complex combinations
 *         of things here, just to catch any accidental strange interactions. For example, have
 *         your function take its input from an index like {@code FROM employees | EVAL foo=MY_FUNCTION(emp_no)}.
 *         It’s probably a good idea to have your function passed as a parameter to another function
 *         like {@code EVAL foo=MOST(0, MY_FUNCTION(emp_no))}. And likely useful to try the reverse
 *         like {@code EVAL foo=MY_FUNCTION(MOST(languages + 10000, emp_no)}.
 *     </li>
 *     <li>
 *         Now it’s time to make a unit test! The infrastructure for these is under some flux at
 *         the moment, but it’s good to extend {@code AbstractScalarFunctionTestCase}. All of
 *         these tests are parameterized and expect to spend some time finding good parameters.
 *         Also add serialization tests that extend {@code AbstractExpressionSerializationTests<>}.
 *         And also add type error tests that extends {@code ErrorsForCasesWithoutExamplesTestCase}.
 *     </li>
 *     <li>
 *         Once you are happy with the tests run the auto formatter:
 *         {@code ./gradlew -p x-pack/plugin/esql/ spotlessApply}
 *     </li>
 *     <li>
 *         Now you can run all of the ESQL tests like CI:
 *         {@code ./gradlew -p x-pack/plugin/esql/ test}
 *     </li>
 *     <li>
 *         Now it’s time to generate some docs!
 *         Actually, running the tests in the example above should have done it for you.
 *         The generated files are
 *         <ul>
 *              <li>{@code docs/reference/esql/functions/description/myfunction.asciidoc}</li>
 *              <li>{@code docs/reference/esql/functions/examples/myfunction.asciidoc}</li>
 *              <li>{@code docs/reference/esql/functions/layout/myfunction.asciidoc}</li>
 *              <li>{@code docs/reference/esql/functions/parameters/myfunction.asciidoc}</li>
 *              <li>{@code docs/reference/esql/functions/signature/myfunction.svg}</li>
 *              <li>{@code docs/reference/esql/functions/types/myfunction.asciidoc}</li>
 *              <li>{@code docs/reference/esql/functions/kibana/definition/myfunction.json}</li>
 *              <li>{@code docs/reference/esql/functions/kibana/docs/myfunction.asciidoc}</li>
 *         </ul>
 *
 *         Make sure to commit them. Add a reference to the
 *         {@code docs/reference/esql/functions/layout/myfunction.asciidoc} in the function list
 *         docs. There are plenty of examples on how
 *         to reference those files e.g. if you are writing a Math function, you will want to
 *         list it in {@code docs/reference/esql/functions/math-functions.asciidoc}.
 *         <p>
 *             You can generate the docs for just your function by running
 *             {@code ./gradlew :x-pack:plugin:esql:test -Dtests.class='*SinTests'}. It’s just
 *             running your new unit test. You should see something like:
 *         </p>
 *         <pre>{@code
 *              > Task :x-pack:plugin:esql:test
 *              ESQL Docs: Only files related to [sin.asciidoc], patching them into place
 *         }</pre>
 *     </li>
 *     <li>
 *          Build the docs by cloning the <a href="https://github.com/elastic/docs">docs repo</a>
 *          and running:
 *          <pre>{@code
 * ../docs/build_docs --doc docs/reference/index.asciidoc --open --chunk 1
 *          }</pre>
 *          from the elasticsearch directory. The first time you run the docs build it does a bunch
 *          of things with docker to get itself ready. Hopefully you can sit back and watch the show.
 *          It won’t need to do it a second time unless some poor soul updates the Dockerfile in the
 *          docs repo.
 *     </li>
 *     <li>
 *         When it finishes building it'll open a browser window. Go to the
 *         <a href="http://localhost:8000/guide/esql-functions.html">functions page</a> to see your
 *         function in the list and follow it’s link to get to the page you built. Make sure it
 *         looks ok.
 *     </li>
 *     <li>
 *         Let’s finish up the code by making the tests backwards compatible. Since this is a new
 *         feature we just have to convince the tests not to run in a cluster that includes older
 *         versions of Elasticsearch. We do that with a {@link org.elasticsearch.rest.RestHandler#supportedCapabilities capability}
 *         on the REST handler. ESQL has a <strong>ton</strong> of capabilities so we list them
 *         all in {@link org.elasticsearch.xpack.esql.action.EsqlCapabilities}. Add a new one
 *         for your function. Now add something like {@code required_capability: my_function}
 *         to all of your csv-spec tests. Run those csv-spec tests as integration tests to double
 *         check that they run on the main branch.
 *         <br><br>
 *         **Note:** you may notice tests gated based on Elasticsearch version. This was the old way
 *         of doing things. Now, we use specific capabilities for each function.
 *     </li>
 *     <li>
 *         Open the PR. The subject and description of the PR are important because those'll turn
 *         into the commit message we see in the commit history. Good PR descriptions make me very
 *         happy. But functions don’t need an essay.
 *     </li>
 *     <li>
 *         Add the {@code >enhancement} and {@code :Analytics/ES|QL} tags if you are able.
 *         Request a review if you can, probably from one of the folks that github proposes to you.
 *     </li>
 *     <li>
 *         CI might fail for random looking reasons. The first thing you should do is merge {@code main}
 *         into your PR branch. That’s usually just:
 *         <pre>{@code
 * git checkout main && git pull elastic main && git checkout mybranch && git merge main
 *         }</pre>
 *         Don’t worry about the commit message. It'll get squashed away in the merge.
 *     </li>
 * </ol>
 */
package org.elasticsearch.xpack.esql.expression.function.scalar;
