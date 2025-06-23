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
 *         Aggregation functions are more complex than scalar functions, so it’s a good idea to discuss
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
 *         isn’t a strong guiding principle in the organization.
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
 *             <li>
 *                 Implement {@link org.elasticsearch.xpack.esql.expression.SurrogateExpression}, and its required
 *                 {@link org.elasticsearch.xpack.esql.expression.SurrogateExpression#surrogate()} method.
 *                 <p>
 *                     It’s used to be able to fold the aggregation when it receives only literals,
 *                     or when the aggregation can be simplified.
 *                 </p>
 *             </li>
 *         </ul>
 *
 *         Finally, implement {@link org.elasticsearch.xpack.esql.planner.ToAggregator} (More information about aggregators below).
 *         The only case when this interface is not required is when it always returns another function in its surrogate.
 *     </li>
 *     <li>
 *         To introduce your aggregation to the engine:
 *         <ul>
 *             <li>
 *                 Implement serialization for your aggregation by implementing
 *                 {@link org.elasticsearch.common.io.stream.NamedWriteable#getWriteableName},
 *                 {@link org.elasticsearch.common.io.stream.NamedWriteable#writeTo},
 *                 and a deserializing constructor. Then add an {@link org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry}
 *                 constant and add that constant to the list in
 *                 {@link org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateWritables#getNamedWriteables}.
 *             </li>
 *             <li>
 *                 Add it to {@link org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry}.
 *             </li>
 *         </ul>
 *     </li>
 * </ol>
 *
 * <h3>Creating aggregators for your function</h3>
 * <p>
 *     Aggregators contain the core logic of how to combine values, what to store, how to process data, etc.
 *     Currently, we rely on code generation (per aggregation per type) in order to implement such functionality.
 *     This approach was picked for performance reasons (namely to avoid virtual method calls and boxing types).
 *     As a result we could not rely on interfaces implementation and generics.
 * </p>
 * <p>
 *     In order to implement aggregation logic create your class (typically named "${FunctionName}${Type}Aggregator").
 *     It must be placed in `org.elasticsearch.compute.aggregation` in order to be picked up by code generation.
 *     Annotate it with {@link org.elasticsearch.compute.ann.Aggregator} and {@link org.elasticsearch.compute.ann.GroupingAggregator}
 *     The first one is responsible for an entire data set aggregation, while the second one is responsible for grouping within buckets.
 * </p>
 * <h4>Before you start implementing it, please note that:</h4>
 * <ul>
 *     <li>All methods must be public static</li>
 *     <li>
 *         {@code init/initSingle/initGrouping} could have optional {@link org.elasticsearch.common.util.BigArrays} or
 *         {@link org.elasticsearch.compute.operator.DriverContext} arguments that are going to be injected automatically.
 *         It is also possible to declare any number of arbitrary arguments that must be provided via generated Supplier.
 *     </li>
 *     <li>
 *         {@code combine, combineStates, combineIntermediate, evaluateFinal} methods (see below) could be generated automatically
 *         when both input type I and mutable accumulator state AggregatorState and GroupingAggregatorState are primitive (DOUBLE, INT).
 *     </li>
 *     <li>
 *         Code generation expects at least one IntermediateState field that is going to be used to keep
 *         the serialized state of the aggregation (eg AggregatorState and GroupingAggregatorState).
 *         It must be defined even if you rely on autogenerated implementation for the primitive types.
 *     </li>
 * </ul>
 * <h4>Aggregation expects:</h4>
 * <ul>
 *     <li>
 *         type AggregatorState (a mutable state used to accumulate result of the aggregation) to be public, not inner and implements
 *         {@link org.elasticsearch.compute.aggregation.AggregatorState}
 *     </li>
 *     <li>type I (input to your aggregation function), usually primitive types and {@link org.apache.lucene.util.BytesRef}</li>
 *     <li>{@code AggregatorState init()} or {@code AggregatorState initSingle()} returns empty initialized aggregation state</li>
 *     <li>
 *         {@code void combine(AggregatorState state, I input)} or {@code AggregatorState combine(AggregatorState state, I input)}
 *         adds input entry to the aggregation state
 *     </li>
 *     <li>
 *         {@code void combineIntermediate(AggregatorState state, intermediate states)} adds serialized aggregation state
 *         to the current aggregation state (used to combine results across different nodes)
 *     </li>
 *     <li>
 *         {@code Block evaluateFinal(AggregatorState state, DriverContext)} converts the inner state of the aggregation to the result
 *         column
 *     </li>
 * </ul>
 * <h4>Grouping aggregation expects:</h4>
 * <ul>
 *     <li>
 *         type GroupingAggregatorState (a mutable state used to accumulate result of the grouping aggregation) to be public,
 *         not inner and implements {@link org.elasticsearch.compute.aggregation.GroupingAggregatorState}
 *     </li>
 *     <li>type I (input to your aggregation function), usually primitive types and {@link org.apache.lucene.util.BytesRef}</li>
 *     <li>
 *         {@code GroupingAggregatorState init()} or {@code GroupingAggregatorState initGrouping()} returns empty initialized grouping
 *         aggregation state
 *     </li>
 *     <li>
 *         {@code void combine(GroupingAggregatorState state, int groupId, I input)} adds input entry to the corresponding group (bucket)
 *         of the grouping aggregation state
 *     </li>
 *     <li>
 *         {@code void combineStates(GroupingAggregatorState targetState, int targetGroupId, GS otherState, int otherGroupId)}
 *         merges other grouped aggregation state into the first one
 *     </li>
 *     <li>
 *         {@code void combineIntermediate(GroupingAggregatorState current, int groupId, intermediate states)} adds serialized
 *         aggregation state to the current grouped aggregation state (used to combine results across different nodes)
 *     </li>
 *     <li>
 *         {@code Block evaluateFinal(GroupingAggregatorState state, IntVectorSelected, DriverContext)} converts the inner state
 *         of the grouping aggregation to the result column
 *     </li>
 * </ul>
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
 *         Implement (or create an empty) methods according to the above list.
 *         Also check {@link org.elasticsearch.compute.ann.Aggregator} JavaDoc as it contains generated method usage.
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
 *         Code generation is triggered when running the tests.
 *         Run the CsvTests to generate the code. Generated code should include:
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
 *             You may need to tweak some import orders per type so they don’t raise warnings.
 *         </p>
 *     </li>
 * </ol>
 */
package org.elasticsearch.xpack.esql.expression.function.aggregate;
