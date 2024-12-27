/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.scalar.spatial.StDistance;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.io.IOException;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.GEO_POINT;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSourceTests.TestPhysicalPlanBuilder.from;
import static org.elasticsearch.xpack.esql.plan.physical.AbstractPhysicalPlanSerializationTests.randomEstimatedRowSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class PushTopNToSourceTests extends ESTestCase {

    public void testSimpleSortField() {
        // FROM index | SORT field | LIMIT 10
        var query = from("index").sort("field").limit(10);
        assertPushdownSort(query);
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleScoreSortField() {
        // FROM index METADATA _score | SORT _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false).scoreSort().limit(10);
        assertPushdownSort(query);
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortMultipleFields() {
        // FROM index | SORT field, integer, double | LIMIT 10
        var query = from("index").sort("field").sort("integer").sort("double").limit(10);
        assertPushdownSort(query);
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortMultipleFieldsAndScore() {
        // FROM index | SORT field, integer, double, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false).sort("field").sort("integer").sort("double").scoreSort().limit(10);
        assertPushdownSort(query);
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldAndEvalLiteral() {
        // FROM index | EVAL x = 1 | SORT field | LIMIT 10
        var query = from("index").eval("x", e -> e.i(1)).sort("field").limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldScoreAndEvalLiteral() {
        // FROM index METADATA _score | EVAL x = 1 | SORT field, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false).eval("x", e -> e.i(1)).sort("field").scoreSort().limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldWithAlias() {
        // FROM index | EVAL x = field | SORT field | LIMIT 10
        var query = from("index").eval("x", b -> b.field("field")).sort("field").limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortMultipleFieldsWithAliases() {
        // FROM index | EVAL x = field, y = integer, z = double | SORT field, integer, double | LIMIT 10
        var query = from("index").eval("x", b -> b.field("field"))
            .eval("y", b -> b.field("integer"))
            .eval("z", b -> b.field("double"))
            .sort("field")
            .sort("integer")
            .sort("double")
            .limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortMultipleFieldsWithAliasesAndScore() {
        // FROM index | EVAL x = field, y = integer, z = double | SORT field, integer, double, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("x", b -> b.field("field"))
            .eval("y", b -> b.field("integer"))
            .eval("z", b -> b.field("double"))
            .sort("field")
            .sort("integer")
            .sort("double")
            .scoreSort()
            .limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldAsAlias() {
        // FROM index | EVAL x = field | SORT x | LIMIT 10
        var query = from("index").eval("x", b -> b.field("field")).sort("x").limit(10);
        assertPushdownSort(query, Map.of("x", "field"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldAsAliasAndScore() {
        // FROM index METADATA _score | EVAL x = field | SORT x, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false).eval("x", b -> b.field("field")).sort("x").scoreSort().limit(10);
        assertPushdownSort(query, Map.of("x", "field"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldAndEvalSumLiterals() {
        // FROM index | EVAL sum = 1 + 2 | SORT field | LIMIT 10
        var query = from("index").eval("sum", b -> b.add(b.i(1), b.i(2))).sort("field").limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldAndEvalSumLiteralsAndScore() {
        // FROM index METADATA _score | EVAL sum = 1 + 2 | SORT field, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("sum", b -> b.add(b.i(1), b.i(2)))
            .sort("field")
            .scoreSort()
            .limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldAndEvalSumLiteralAndField() {
        // FROM index | EVAL sum = 1 + integer | SORT integer | LIMIT 10
        var query = from("index").eval("sum", b -> b.add(b.i(1), b.field("integer"))).sort("integer").limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortFieldAndEvalSumLiteralAndFieldAndScore() {
        // FROM index METADATA _score | EVAL sum = 1 + integer | SORT integer, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("sum", b -> b.add(b.i(1), b.field("integer")))
            .sort("integer")
            .scoreSort()
            .limit(10);
        assertPushdownSort(query, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSimpleSortEvalSumLiteralAndField() {
        // FROM index | EVAL sum = 1 + integer | SORT sum | LIMIT 10
        var query = from("index").eval("sum", b -> b.add(b.i(1), b.field("integer"))).sort("sum").limit(10);
        // TODO: Consider supporting this if we can determine that the eval function maintains the same order
        assertNoPushdownSort(query, "when sorting on a derived field");
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/114515")
    public void testPartiallyPushableSort() {
        // FROM index | EVAL sum = 1 + integer | SORT integer, sum, field | LIMIT 10
        var query = from("index").eval("sum", b -> b.add(b.i(1), b.field("integer"))).sort("integer").sort("sum").sort("field").limit(10);
        // Both integer and field can be pushed down, but we can only push down the leading sortable fields, so the 'sum' blocks 'field'
        assertPushdownSort(query, List.of(query.orders.get(0)), null, List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoPointField() {
        // FROM index | SORT location | LIMIT 10
        var query = from("index").sort("location", Order.OrderDirection.ASC).limit(10);
        // NOTE: while geo_point is not sortable, this is checked during logical planning and the physical planner does not know or care
        assertPushdownSort(query);
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoPointFieldAnsScore() {
        // FROM index METADATA _score | SORT location, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false).sort("location", Order.OrderDirection.ASC).scoreSort().limit(10);
        // NOTE: while geo_point is not sortable, this is checked during logical planning and the physical planner does not know or care
        assertPushdownSort(query);
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunction() {
        // FROM index | EVAL distance = ST_DISTANCE(location, POINT(1 2)) | SORT distance | LIMIT 10
        var query = from("index").eval("distance", b -> b.distance("location", "POINT(1 2)"))
            .sort("distance", Order.OrderDirection.ASC)
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, Map.of("distance", "location"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionAndScore() {
        // FROM index METADATA _score | EVAL distance = ST_DISTANCE(location, POINT(1 2)) | SORT distance, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("distance", b -> b.distance("location", "POINT(1 2)"))
            .sort("distance", Order.OrderDirection.ASC)
            .scoreSort()
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, Map.of("distance", "location"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionInverted() {
        // FROM index | EVAL distance = ST_DISTANCE(POINT(1 2), location) | SORT distance | LIMIT 10
        var query = from("index").eval("distance", b -> b.distance("POINT(1 2)", "location"))
            .sort("distance", Order.OrderDirection.ASC)
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, Map.of("distance", "location"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionInvertedAndScore() {
        // FROM index METADATA _score | EVAL distance = ST_DISTANCE(POINT(1 2), location) | SORT distance, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("distance", b -> b.distance("POINT(1 2)", "location"))
            .sort("distance", Order.OrderDirection.ASC)
            .scoreSort()
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, Map.of("distance", "location"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionLiterals() {
        // FROM index | EVAL distance = ST_DISTANCE(POINT(2 1), POINT(1 2)) | SORT distance | LIMIT 10
        var query = from("index").eval("distance", b -> b.distance("POINT(2 1)", "POINT(1 2)"))
            .sort("distance", Order.OrderDirection.ASC)
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertNoPushdownSort(query, "sort on foldable distance function");
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionLiteralsAndScore() {
        // FROM index METADATA _score | EVAL distance = ST_DISTANCE(POINT(2 1), POINT(1 2)) | SORT distance, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("distance", b -> b.distance("POINT(2 1)", "POINT(1 2)"))
            .sort("distance", Order.OrderDirection.ASC)
            .scoreSort()
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertNoPushdownSort(query, "sort on foldable distance function");
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionAndFieldsWithAliases() {
        // FROM index | EVAL distance = ST_DISTANCE(location, POINT(1 2)), x = field | SORT distance, field, integer | LIMIT 10
        var query = from("index").eval("distance", b -> b.distance("location", "POINT(1 2)"))
            .eval("x", b -> b.field("field"))
            .sort("distance", Order.OrderDirection.ASC)
            .sort("field", Order.OrderDirection.DESC)
            .sort("integer", Order.OrderDirection.DESC)
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, query.orders, Map.of("distance", "location"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionAndFieldsWithAliasesAndScore() {
        // FROM index | EVAL distance = ST_DISTANCE(location, POINT(1 2)), x = field | SORT distance, field, integer, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("distance", b -> b.distance("location", "POINT(1 2)"))
            .eval("x", b -> b.field("field"))
            .sort("distance", Order.OrderDirection.ASC)
            .sort("field", Order.OrderDirection.DESC)
            .sort("integer", Order.OrderDirection.DESC)
            .scoreSort()
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, query.orders, Map.of("distance", "location"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionAndFieldsAndAliases() {
        // FROM index | EVAL distance = ST_DISTANCE(location, POINT(1 2)), x = field | SORT distance, x, integer | LIMIT 10
        var query = from("index").eval("distance", b -> b.distance("location", "POINT(1 2)"))
            .eval("x", b -> b.field("field"))
            .sort("distance", Order.OrderDirection.ASC)
            .sort("x", Order.OrderDirection.DESC)
            .sort("integer", Order.OrderDirection.DESC)
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, query.orders, Map.of("distance", "location", "x", "field"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionAndFieldsAndAliasesAndScore() {
        // FROM index | EVAL distance = ST_DISTANCE(location, POINT(1 2)), x = field | SORT distance, x, integer, _score | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("distance", b -> b.distance("location", "POINT(1 2)"))
            .eval("x", b -> b.field("field"))
            .sort("distance", Order.OrderDirection.ASC)
            .sort("x", Order.OrderDirection.DESC)
            .sort("integer", Order.OrderDirection.DESC)
            .scoreSort()
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, query.orders, Map.of("distance", "location", "x", "field"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionAndFieldsAndManyAliases() {
        // FROM index
        // | EVAL loc = location, loc2 = loc, loc3 = loc2, distance = ST_DISTANCE(loc3, POINT(1 2)), x = field
        // | SORT distance, x, integer
        // | LIMIT 10
        var query = from("index").eval("loc", b -> b.field("location"))
            .eval("loc2", b -> b.ref("loc"))
            .eval("loc3", b -> b.ref("loc2"))
            .eval("distance", b -> b.distance("loc3", "POINT(1 2)"))
            .eval("x", b -> b.field("field"))
            .sort("distance", Order.OrderDirection.ASC)
            .sort("x", Order.OrderDirection.DESC)
            .sort("integer", Order.OrderDirection.DESC)
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, Map.of("distance", "location", "x", "field"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    public void testSortGeoDistanceFunctionAndFieldsAndManyAliasesAndScore() {
        // FROM index METADATA _score
        // | EVAL loc = location, loc2 = loc, loc3 = loc2, distance = ST_DISTANCE(loc3, POINT(1 2)), x = field
        // | SORT distance, x, integer, _score
        // | LIMIT 10
        var query = from("index").metadata("_score", DOUBLE, false)
            .eval("loc", b -> b.field("location"))
            .eval("loc2", b -> b.ref("loc"))
            .eval("loc3", b -> b.ref("loc2"))
            .eval("distance", b -> b.distance("loc3", "POINT(1 2)"))
            .eval("x", b -> b.field("field"))
            .sort("distance", Order.OrderDirection.ASC)
            .sort("x", Order.OrderDirection.DESC)
            .sort("integer", Order.OrderDirection.DESC)
            .scoreSort()
            .limit(10);
        // The pushed-down sort will use the underlying field 'location', not the sorted reference field 'distance'
        assertPushdownSort(query, Map.of("distance", "location", "x", "field"), List.of(EvalExec.class, EsQueryExec.class));
        assertNoPushdownSort(query.asTimeSeries(), "for time series index mode");
    }

    private static void assertPushdownSort(TestPhysicalPlanBuilder builder) {
        assertPushdownSort(builder, null, List.of(EsQueryExec.class));
    }

    private static void assertPushdownSort(TestPhysicalPlanBuilder builder, List<Class<? extends PhysicalPlan>> topClass) {
        assertPushdownSort(builder, null, topClass);
    }

    private static void assertPushdownSort(
        TestPhysicalPlanBuilder builder,
        Map<String, String> fieldMap,
        List<Class<? extends PhysicalPlan>> topClass
    ) {
        var topNExec = builder.build();
        var result = pushTopNToSource(topNExec);
        assertPushdownSort(result, builder.orders, fieldMap, topClass);
    }

    private static void assertPushdownSort(
        TestPhysicalPlanBuilder builder,
        List<Order> expectedSorts,
        Map<String, String> fieldMap,
        List<Class<? extends PhysicalPlan>> topClass
    ) {
        var topNExec = builder.build();
        var result = pushTopNToSource(topNExec);
        assertPushdownSort(result, expectedSorts, fieldMap, topClass);
    }

    private static void assertNoPushdownSort(TestPhysicalPlanBuilder builder, String message) {
        var topNExec = builder.build();
        var result = pushTopNToSource(topNExec);
        assertNoPushdownSort(result, message);
    }

    private static PhysicalPlan pushTopNToSource(TopNExec topNExec) {
        var configuration = EsqlTestUtils.configuration("from test");
        var ctx = new LocalPhysicalOptimizerContext(configuration, SearchStats.EMPTY);
        var pushTopNToSource = new PushTopNToSource();
        return pushTopNToSource.rule(topNExec, ctx);
    }

    private static void assertNoPushdownSort(PhysicalPlan plan, String message) {
        var esQueryExec = findEsQueryExec(plan);
        var sorts = esQueryExec.sorts();
        assertThat("Expect no sorts " + message, sorts.size(), is(0));
    }

    private static void assertPushdownSort(
        PhysicalPlan plan,
        List<Order> expectedSorts,
        Map<String, String> fieldMap,
        List<Class<? extends PhysicalPlan>> topClass
    ) {
        if (topClass != null && topClass.size() > 0) {
            PhysicalPlan current = plan;
            for (var clazz : topClass) {
                assertThat("Expect non-null physical plan class to match " + clazz.getSimpleName(), current, notNullValue());
                assertThat("Expect top physical plan class to match", current.getClass(), is(clazz));
                current = current.children().size() > 0 ? current.children().get(0) : null;
            }
            if (current != null) {
                fail("No more child classes expected in plan, but found: " + current.getClass().getSimpleName());
            }
        }
        var esQueryExec = findEsQueryExec(plan);
        var sorts = esQueryExec.sorts();
        assertThat("Expect sorts count to match", sorts.size(), is(expectedSorts.size()));
        for (int i = 0; i < expectedSorts.size(); i++) {
            String name = ((Attribute) expectedSorts.get(i).child()).name();
            EsQueryExec.Sort sort = sorts.get(i);
            if (sort.field() != null) {
                String fieldName = sort.field().fieldName();
                assertThat("Expect sort[" + i + "] name to match", fieldName, is(sortName(name, fieldMap)));
            }
            assertThat("Expect sort[" + i + "] direction to match", sort.direction(), is(expectedSorts.get(i).direction()));
        }
    }

    private static String sortName(String name, Map<String, String> fieldMap) {
        return fieldMap != null ? fieldMap.getOrDefault(name, name) : name;
    }

    private static EsQueryExec findEsQueryExec(PhysicalPlan plan) {
        if (plan instanceof EsQueryExec esQueryExec) {
            return esQueryExec;
        }
        // We assume no physical plans with multiple children would be generated
        return findEsQueryExec(plan.children().get(0));
    }

    /**
     * This builder allows for easy creation of physical plans using a syntax like `from("index").sort("field").limit(10)`.
     * The idea is to create tests that are clearly related to real queries, but also easy to make assertions on.
     * It only supports a very small subset of possible plans, with FROM, EVAL and SORT+LIMIT, in that order, matching
     * the physical plan rules that are being tested: TopNExec, EvalExec and EsQueryExec.
     */
    static class TestPhysicalPlanBuilder {
        private final String index;
        private final LinkedHashMap<String, FieldAttribute> fields;
        private final LinkedHashMap<String, ReferenceAttribute> refs;
        private final LinkedHashMap<String, MetadataAttribute> metadata;
        private IndexMode indexMode;
        private final List<Alias> aliases = new ArrayList<>();
        private final List<Order> orders = new ArrayList<>();
        private int limit = Integer.MAX_VALUE;

        private TestPhysicalPlanBuilder(String index, IndexMode indexMode) {
            this.index = index;
            this.indexMode = indexMode;
            this.fields = new LinkedHashMap<>();
            this.refs = new LinkedHashMap<>();
            this.metadata = new LinkedHashMap<>();
            addSortableFieldAttributes(this.fields);
        }

        private static void addSortableFieldAttributes(Map<String, FieldAttribute> fields) {
            addFieldAttribute(fields, "field", KEYWORD);
            addFieldAttribute(fields, "integer", INTEGER);
            addFieldAttribute(fields, "double", DOUBLE);
            addFieldAttribute(fields, "keyword", KEYWORD);
            addFieldAttribute(fields, "location", GEO_POINT);
        }

        private static void addFieldAttribute(Map<String, FieldAttribute> fields, String name, DataType type) {
            fields.put(name, new FieldAttribute(Source.EMPTY, name, new EsField(name, type, new HashMap<>(), true)));
        }

        static TestPhysicalPlanBuilder from(String index) {
            return new TestPhysicalPlanBuilder(index, IndexMode.STANDARD);
        }

        TestPhysicalPlanBuilder metadata(String metadataAttribute, DataType dataType, boolean searchable) {
            metadata.put(metadataAttribute, new MetadataAttribute(Source.EMPTY, metadataAttribute, dataType, searchable));
            return this;
        }

        public TestPhysicalPlanBuilder eval(Alias... aliases) {
            if (orders.isEmpty() == false) {
                throw new IllegalArgumentException("Eval must be before sort");
            }
            if (aliases.length == 0) {
                throw new IllegalArgumentException("At least one alias must be provided");
            }
            for (Alias alias : aliases) {
                if (refs.containsKey(alias.name())) {
                    throw new IllegalArgumentException("Reference already exists: " + alias.name());
                }
                refs.put(
                    alias.name(),
                    new ReferenceAttribute(Source.EMPTY, alias.name(), alias.dataType(), Nullability.FALSE, alias.id(), alias.synthetic())
                );
                this.aliases.add(alias);
            }
            return this;
        }

        public TestPhysicalPlanBuilder eval(String name, Function<TestExpressionBuilder, Expression> builder) {
            var testExpressionBuilder = new TestExpressionBuilder();
            Expression expression = builder.apply(testExpressionBuilder);
            return eval(new Alias(Source.EMPTY, name, expression));
        }

        public TestPhysicalPlanBuilder sort(String field) {
            return sort(field, Order.OrderDirection.ASC);
        }

        public TestPhysicalPlanBuilder scoreSort(Order.OrderDirection direction) {
            orders.add(
                new Order(
                    Source.EMPTY,
                    MetadataAttribute.create(Source.EMPTY, MetadataAttribute.SCORE),
                    direction,
                    Order.NullsPosition.LAST
                )
            );
            return this;
        }

        public TestPhysicalPlanBuilder scoreSort() {
            return scoreSort(Order.OrderDirection.DESC);
        }

        public TestPhysicalPlanBuilder sort(String field, Order.OrderDirection direction) {
            Attribute attr = refs.get(field);
            if (attr == null) {
                attr = fields.get(field);
            }
            if (attr == null) {
                throw new IllegalArgumentException("Field not found: " + field);
            }
            orders.add(new Order(Source.EMPTY, attr, direction, Order.NullsPosition.LAST));
            return this;
        }

        public TestPhysicalPlanBuilder limit(int limit) {
            this.limit = limit;
            return this;
        }

        public TopNExec build() {
            EsIndex esIndex = new EsIndex(this.index, Map.of());
            List<Attribute> attributes = new ArrayList<>(fields.values());
            PhysicalPlan child = new EsQueryExec(Source.EMPTY, esIndex, indexMode, attributes, null, null, List.of(), 0);
            if (aliases.isEmpty() == false) {
                child = new EvalExec(Source.EMPTY, child, aliases);
            }
            return new TopNExec(Source.EMPTY, child, orders, new Literal(Source.EMPTY, limit, INTEGER), randomEstimatedRowSize());
        }

        public TestPhysicalPlanBuilder asTimeSeries() {
            this.indexMode = IndexMode.TIME_SERIES;
            return this;
        }

        class TestExpressionBuilder {
            Expression field(String name) {
                return fields.get(name);
            }

            Expression ref(String name) {
                return refs.get(name);
            }

            Expression literal(Object value, DataType dataType) {
                return new Literal(Source.EMPTY, value, dataType);
            }

            Expression i(int value) {
                return new Literal(Source.EMPTY, value, DataType.INTEGER);
            }

            Expression d(double value) {
                return new Literal(Source.EMPTY, value, DOUBLE);
            }

            Expression k(String value) {
                return new Literal(Source.EMPTY, value, KEYWORD);
            }

            public Expression add(Expression left, Expression right) {
                return new Add(Source.EMPTY, left, right);
            }

            public Expression distance(String left, String right) {
                return new StDistance(Source.EMPTY, geoExpr(left), geoExpr(right));
            }

            private Expression geoExpr(String text) {
                if (text.startsWith("POINT")) {
                    try {
                        Geometry geometry = WellKnownText.fromWKT(GeometryValidator.NOOP, false, text);
                        BytesRef bytes = new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
                        return new Literal(Source.EMPTY, bytes, GEO_POINT);
                    } catch (IOException | ParseException e) {
                        throw new IllegalArgumentException("Failed to parse WKT: " + text, e);
                    }
                }
                if (fields.containsKey(text)) {
                    return fields.get(text);
                }
                if (refs.containsKey(text)) {
                    return refs.get(text);
                }
                throw new IllegalArgumentException("Unknown field: " + text);
            }
        }

    }
}
