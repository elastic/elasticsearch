/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * Functions that aggregate values, with or without grouping within buckets.
 * Used in `STATS` and similar commands.
 *
 * <h2>Guide to adding new aggregate function</h2>
 * <ol>
 *     <li>
 *         Aggregation functions are more complex than scalar functions, so it's a good idea to discuss
 *         the new function with the ESQL team before starting to implement it.
 *         <p>
 *             You may also discuss its implementation, as aggregations may require special performance considerations.
 *         </p>
 *     </li>
 *     <li>
 *         To learn the basics about making functions, check {@link org.elasticsearch.xpack.esql.expression.function.scalar}.
 *         <p>
 *             It has the guide to making a simple function, which should be a good base to start doing aggregations.
 *         </p>
 *     </li>
 *     <li>
 *         Pick one of the csv-spec files in {@code x-pack/plugin/esql/qa/testFixtures/src/main/resources/}
 *         and add a test for the function you want to write. These files are roughly themed but there
 *         isn't a strong guiding principle in the organization.
 *     </li>
 *     <li>
 *         Rerun the {@code CsvTests} and watch your new test fail.
 *     </li>
 *     <li>
 *         Find an aggregate function in this package similar to the one you are working on and copy it to build
 *         yours.
 *         Your function might extend from the available abstract classes. Check the javadoc of each before using them:
 *         <ul>
 *             <li>
 *                 {@link org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction}: The base class for aggregates
 *             </li>
 *             <li>
 *                 {@link org.elasticsearch.xpack.esql.expression.function.aggregate.NumericAggregate}: Aggregation for numeric values
 *             </li>
 *             <li>
 *                 {@link org.elasticsearch.xpack.esql.expression.function.aggregate.SpatialAggregateFunction}:
 *                 Aggregation for spatial values
 *             </li>
 *         </ul>
 *     </li>
 *     <li>
 *         Fill the required methods in your new function. Check their JavaDoc for more information.
 *         Here are some of the important ones:
 *         <ul>
 *             <li>
 *                 Constructor: Review the constructor annotations, and make sure to add the correct types and descriptions.
 *                 <ul>
 *                     <li>{@link org.elasticsearch.xpack.esql.expression.function.FunctionInfo}, for the constructor itself</li>
 *                     <li>{@link org.elasticsearch.xpack.esql.expression.function.Param}, for the function parameters</li>
 *                 </ul>
 *             </li>
 *             <li>
 *                 {@code resolveType}: Check the metadata of your function parameters.
 *                 This may include types, whether they are foldable or not, or their possible values.
 *             </li>
 *             <li>
 *                 {@code dataType}: This will return the datatype of your function.
 *                 May be based on its current parameters.
 *             </li>
 *         </ul>
 *
 *         Finally, you may want to implement some interfaces.
 *         Check their JavaDocs to see if they are suitable for your function:
 *         <ul>
 *             <li>
 *                 {@link org.elasticsearch.xpack.esql.planner.ToAggregator}: (More information about aggregators below)
 *             </li>
 *             <li>
 *                 {@link org.elasticsearch.xpack.esql.expression.SurrogateExpression}
 *             </li>
 *         </ul>
 *     </li>
 *     <li>
 *         To introduce your aggregation to the engine:
 *         <ul>
 *             <li>
 *                 Add it to {@code org.elasticsearch.xpack.esql.planner.AggregateMapper}.
 *                 Check all usages of other aggregations there, and replicate the logic.
 *             </li>
 *             <li>
 *                 Add it to {@link org.elasticsearch.xpack.esql.io.stream.PlanNamedTypes}.
 *                 Consider adding a {@code writeTo} method and a constructor/{@code readFrom} method inside your function,
 *                 to keep all the logic in one place.
 *                 <p>
 *                     You can find examples of other aggregations using this method,
 *                     like {@link org.elasticsearch.xpack.esql.expression.function.aggregate.Top#writeTo(PlanStreamOutput)}
 *                 </p>
 *             </li>
 *             <li>
 *                 Do the same with {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 *             </li>
 *         </ul>
 *     </li>
 * </ol>
 *
 * <h3>Creating aggregators for your function</h3>
 * <p>
 *     Aggregators contain the core logic of your aggregation. That is, how to combine values, what to store, how to process data, etc.
 * </p>
 * <ol>
 *     <li>
 *         Copy an existing aggregator to use as a base. You'll usually make one per type. Check other classes to see the naming pattern.
 *         You can find them in {@link org.elasticsearch.compute.aggregation}.
 *         <p>
 *             Note that some aggregators are autogenerated, so they live in different directories.
 *             The base is {@code x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/}
 *         </p>
 *     </li>
 *     <li>
 *         The methods in the aggregator will define how it will work:
 *         <ul>
 *             <li>
 *                 Adding the `type init()` method will autogenerate the code to manage the state, using your returned value
 *                 as the initial value for each group.
 *             </li>
 *             <li>
 *                 Adding the `type initSingle()` or `type initGrouping()` methods will use the state object you return there instead.
 *                 <p>
 *                     You will also have to provide `evaluateIntermediate()` and `evaluateFinal()` methods this way.
 *                 </p>
 *             </li>
 *         </ul>
 *         Depending on the way you use, adapt your `combine*()` methods to receive one or other type as their first parameters.
 *     </li>
 *     <li>
 *         If it's also a {@link org.elasticsearch.compute.ann.GroupingAggregator}, you should provide the same methods as commented before:
 *         <ul>
 *             <li>
 *                 Add an `initGrouping()`, unless you're using the `init()` method
 *             </li>
 *             <li>
 *                 Add all the other methods, with the state parameter of the type of your `initGrouping()`.
 *             </li>
 *         </ul>
 *     </li>
 *     <li>
 *         Make a test for your aggregator.
 *         You can copy an existing one from {@code x-pack/plugin/esql/compute/src/test/java/org/elasticsearch/compute/aggregation/}.
 *         <p>
 *             Tests extending from {@code org.elasticsearch.compute.aggregation.AggregatorFunctionTestCase}
 *             will already include most required cases. You should only need to fill the required abstract methods.
 *         </p>
 *     </li>
 *     <li>
 *         Check the Javadoc of the {@link org.elasticsearch.compute.ann.Aggregator}
 *         and {@link org.elasticsearch.compute.ann.GroupingAggregator} annotations.
 *         Add/Modify them on your aggregator.
 *     </li>
 *     <li>
 *         The {@link org.elasticsearch.compute.ann.Aggregator} JavaDoc explains the static methods you should add.
 *     </li>
 *     <li>
 *         After implementing the required methods (Even if they have a dummy implementation),
 *         run the CsvTests to generate some extra required classes.
 *         <p>
 *             One of them will be the {@code AggregatorFunctionSupplier} for your aggregator.
 *             Find it by its name ({@code <Aggregation-name><Type>AggregatorFunctionSupplier}),
 *             and return it in the {@code toSupplier} method in your function, under the correct type condition.
 *         </p>
 *     </li>
 *     <li>
 *         Now, complete the implementation of the aggregator, until the tests pass!
 *     </li>
 * </ol>
 *
 * <h3>StringTemplates</h3>
 * <p>
 *     Making an aggregator per type may be repetitive. To avoid code duplication, we use StringTemplates:
 * </p>
 * <ol>
 *     <li>
 *         Create a new StringTemplate file.
 *         Use another as a reference, like
 *         {@code x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/aggregation/X-TopAggregator.java.st}.
 *     </li>
 *     <li>
 *         Add the template scripts to {@code x-pack/plugin/esql/compute/build.gradle}.
 *         <p>
 *             You can also see there which variables you can use, and which types are currently supported.
 *         </p>
 *     </li>
 *     <li>
 *         After completing your template, run the generation with {@code ./gradlew :x-pack:plugin:esql:compute:compileJava}.
 *         <p>
 *             You may need to tweak some import orders per type so they don't raise warnings.
 *         </p>
 *     </li>
 * </ol>
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
