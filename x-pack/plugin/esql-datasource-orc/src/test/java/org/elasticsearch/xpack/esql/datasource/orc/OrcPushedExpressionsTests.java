/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.orc;

import org.apache.hadoop.hive.ql.io.sarg.PredicateLeaf;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.lucene.util.BytesRef;
import org.apache.orc.TypeDescription;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.predicate.Range;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

import java.sql.Date;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.EsField.TimeSeriesFieldType;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Symmetric matrix tests for {@link OrcPushedExpressions#toSearchArgument(TypeDescription)}
 * over nested STRUCT subfields (e.g. {@code event.action}). Mirrors the Parquet-side
 * {@code ParquetPushedExpressionsTests} nested coverage. The plan's T3 calls these out
 * explicitly: comparison + IN / IsNull / Range / And / Or / Not over dotted paths, plus
 * DATETIME-vs-DATE and DOUBLE-vs-DECIMAL type-override resolution under a STRUCT parent.
 *
 * <p>ORC's {@code SearchArgument.Builder} accepts dotted names natively (see ORC-323).
 * The work here verifies that {@link OrcPushedExpressions#buildColumnTypeMap} resolves
 * the nested leaf so the DATE / DECIMAL overrides keep firing for nested columns.
 */
public class OrcPushedExpressionsTests extends ESTestCase {

    private static final Source SOURCE = Source.EMPTY;

    // -------------------- Comparison matrix --------------------

    public void testNestedEqualsString() {
        TypeDescription schema = nestedKeywordSchema();
        SearchArgument sarg = toSearchArgument(schema, eq("event.action", DataType.KEYWORD, new BytesRef("login")));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.action", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.STRING, leaf.getType());
        assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
        assertEquals("login", leaf.getLiteral());
    }

    public void testNestedNotEqualsString() {
        TypeDescription schema = nestedKeywordSchema();
        SearchArgument sarg = toSearchArgument(schema, neq("event.action", DataType.KEYWORD, new BytesRef("admin")));
        assertNotNull(sarg);
        // NotEquals is rendered as NOT(EQUALS) in SearchArgument; the leaf is still EQUALS,
        // but the expression tree negates it. Verify the leaf shape regardless.
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.action", leaf.getColumnName());
        assertEquals(PredicateLeaf.Operator.EQUALS, leaf.getOperator());
    }

    public void testNestedLessThanLong() {
        TypeDescription schema = nestedLongSchema();
        SearchArgument sarg = toSearchArgument(schema, lt("event.id", DataType.LONG, 100L));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.id", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
        assertEquals(PredicateLeaf.Operator.LESS_THAN, leaf.getOperator());
        assertEquals(100L, leaf.getLiteral());
    }

    public void testNestedGreaterThanLong() {
        TypeDescription schema = nestedLongSchema();
        SearchArgument sarg = toSearchArgument(
            schema,
            new GreaterThan(SOURCE, field("event.id", DataType.LONG), literal(7L, DataType.LONG))
        );
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.id", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
        // GreaterThan is encoded as NOT(LESS_THAN_EQUALS); leaf is LESS_THAN_EQUALS.
        assertEquals(PredicateLeaf.Operator.LESS_THAN_EQUALS, leaf.getOperator());
    }

    public void testNestedIsNull() {
        TypeDescription schema = nestedKeywordSchema();
        SearchArgument sarg = toSearchArgument(schema, new IsNull(SOURCE, field("event.action", DataType.KEYWORD)));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.action", leaf.getColumnName());
        assertEquals(PredicateLeaf.Operator.IS_NULL, leaf.getOperator());
    }

    public void testNestedIsNotNull() {
        TypeDescription schema = nestedKeywordSchema();
        SearchArgument sarg = toSearchArgument(schema, new IsNotNull(SOURCE, field("event.action", DataType.KEYWORD)));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.action", leaf.getColumnName());
        assertEquals(PredicateLeaf.Operator.IS_NULL, leaf.getOperator());
    }

    public void testNestedInList() {
        TypeDescription schema = nestedKeywordSchema();
        Expression inExpr = new In(
            SOURCE,
            field("event.action", DataType.KEYWORD),
            List.of(literal(new BytesRef("login"), DataType.KEYWORD), literal(new BytesRef("logout"), DataType.KEYWORD))
        );
        SearchArgument sarg = toSearchArgument(schema, inExpr);
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.action", leaf.getColumnName());
        assertEquals(PredicateLeaf.Operator.IN, leaf.getOperator());
        assertEquals(2, leaf.getLiteralList().size());
    }

    public void testNestedRangeLong() {
        TypeDescription schema = nestedLongSchema();
        Expression range = new Range(
            SOURCE,
            field("event.id", DataType.LONG),
            literal(10L, DataType.LONG),
            true,
            literal(99L, DataType.LONG),
            true,
            ZoneOffset.UTC
        );
        SearchArgument sarg = toSearchArgument(schema, range);
        assertNotNull(sarg);
        // BETWEEN renders as a single leaf with two literals.
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.id", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
        assertEquals(PredicateLeaf.Operator.BETWEEN, leaf.getOperator());
    }

    // -------------------- Logical operators over nested predicates --------------------

    public void testNestedAndCombinesTwoNestedLeaves() {
        TypeDescription schema = nestedActionAndIdSchema();
        Expression action = eq("event.action", DataType.KEYWORD, new BytesRef("login"));
        Expression idGt = new GreaterThan(SOURCE, field("event.id", DataType.LONG), literal(0L, DataType.LONG));
        SearchArgument sarg = toSearchArgument(schema, new And(SOURCE, action, idGt));
        assertNotNull(sarg);
        assertEquals(2, sarg.getLeaves().size());
        // Order is deterministic (left, right); verify dotted names appear on each leaf.
        assertEquals("event.action", sarg.getLeaves().get(0).getColumnName());
        assertEquals("event.id", sarg.getLeaves().get(1).getColumnName());
    }

    public void testNestedOrTwoNestedKeywords() {
        TypeDescription schema = nestedKeywordSchema();
        Expression a = eq("event.action", DataType.KEYWORD, new BytesRef("login"));
        Expression b = eq("event.action", DataType.KEYWORD, new BytesRef("logout"));
        SearchArgument sarg = toSearchArgument(schema, new Or(SOURCE, a, b));
        assertNotNull(sarg);
        assertEquals(2, sarg.getLeaves().size());
        for (PredicateLeaf leaf : sarg.getLeaves()) {
            assertEquals("event.action", leaf.getColumnName());
        }
    }

    public void testNestedNot() {
        TypeDescription schema = nestedKeywordSchema();
        Expression inner = new NotEquals(SOURCE, field("event.action", DataType.KEYWORD), literal(new BytesRef("x"), DataType.KEYWORD));
        SearchArgument sarg = toSearchArgument(schema, new Not(SOURCE, inner));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.action", leaf.getColumnName());
    }

    // -------------------- Type-override resolution (the bug T3 fixes) --------------------

    public void testNestedDatetimeOnDateColumn() {
        // event.d is DATE under a STRUCT. Without the dotted-path column-type map, the
        // resolver would not see the DATE override and would emit a TIMESTAMP literal,
        // mis-pruning stripes. With T3's recursive map, the DATE override fires.
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("d", TypeDescription.createDate()));
        long millis = 86400000L * 19723;
        SearchArgument sarg = toSearchArgument(schema, eq("event.d", DataType.DATETIME, millis));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.d", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.DATE, leaf.getType());
        assertThat(leaf.getLiteral(), instanceOf(Date.class));
    }

    public void testNestedDatetimeOnTimestampColumnStaysTimestamp() {
        // No-regression: under the STRUCT parent, a TIMESTAMP leaf must still resolve to
        // TIMESTAMP (not be incorrectly overridden to DATE).
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("ts", TypeDescription.createTimestamp()));
        SearchArgument sarg = toSearchArgument(schema, eq("event.ts", DataType.DATETIME, 1700000000000L));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.ts", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.TIMESTAMP, leaf.getType());
    }

    public void testNestedDoubleOnDecimalColumn() {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("price", TypeDescription.createDecimal().withScale(2)));
        SearchArgument sarg = toSearchArgument(schema, eq("event.price", DataType.DOUBLE, 99.99));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.price", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.DECIMAL, leaf.getType());
        assertThat(leaf.getLiteral(), instanceOf(HiveDecimalWritable.class));
    }

    public void testNestedDoubleOnDoubleColumnStaysFloat() {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("score", TypeDescription.createDouble()));
        SearchArgument sarg = toSearchArgument(schema, eq("event.score", DataType.DOUBLE, 0.5));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("event.score", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.FLOAT, leaf.getType());
    }

    // -------------------- Deep path & D2 precedence --------------------

    public void testNestedDeepPathFourLevels() {
        TypeDescription schema = TypeDescription.createStruct()
            .addField(
                "a",
                TypeDescription.createStruct()
                    .addField(
                        "b",
                        TypeDescription.createStruct()
                            .addField("c", TypeDescription.createStruct().addField("d", TypeDescription.createLong()))
                    )
            );
        SearchArgument sarg = toSearchArgument(schema, eq("a.b.c.d", DataType.LONG, 42L));
        assertNotNull(sarg);
        PredicateLeaf leaf = sarg.getLeaves().get(0);
        assertEquals("a.b.c.d", leaf.getColumnName());
        assertEquals(PredicateLeaf.Type.LONG, leaf.getType());
    }

    public void testBuildColumnTypeMapRecursesAndAppliesD2Precedence() {
        // The literal top-level "event.action" (STRING) wins over the nested struct walk
        // which would also produce an entry at "event.action" (INT). Mirrors the Parquet
        // testLiteralDottedNameWinsOverPath rule.
        TypeDescription literalThenNested = TypeDescription.createStruct()
            .addField("event.action", TypeDescription.createString())
            .addField("event", TypeDescription.createStruct().addField("action", TypeDescription.createInt()));

        Map<String, TypeDescription.Category> map = OrcPushedExpressions.buildColumnTypeMap(literalThenNested);
        // Literal wins:
        assertEquals(TypeDescription.Category.STRING, map.get("event.action"));
        // The intermediate STRUCT entry is still published for the nested side:
        assertEquals(TypeDescription.Category.STRUCT, map.get("event"));
    }

    public void testBuildColumnTypeMapEnumeratesNestedLeaves() {
        TypeDescription schema = TypeDescription.createStruct()
            .addField("id", TypeDescription.createLong())
            .addField(
                "event",
                TypeDescription.createStruct()
                    .addField("action", TypeDescription.createString())
                    .addField("id", TypeDescription.createLong())
            );
        Map<String, TypeDescription.Category> map = OrcPushedExpressions.buildColumnTypeMap(schema);
        // Top-level entries preserved (no regression).
        assertEquals(TypeDescription.Category.LONG, map.get("id"));
        // STRUCT marker present at the parent path.
        assertEquals(TypeDescription.Category.STRUCT, map.get("event"));
        // Nested leaves present with the dotted path.
        assertEquals(TypeDescription.Category.STRING, map.get("event.action"));
        assertEquals(TypeDescription.Category.LONG, map.get("event.id"));
    }

    // -------------------- Declared-retype physical-type guard (F-PUSH-KW) --------------------

    public void testDeclaredKeywordOverPhysicalLongDeclinesPushdown() {
        // A STRING leaf over a physical LONG mis-prunes (ORC stringifies the integer stats, so
        // == "42" is tested lexicographically against stringified integer bounds). The guard drops
        // the only conjunct -> no SearchArgument.
        TypeDescription schema = TypeDescription.createStruct().addField("code", TypeDescription.createLong());
        assertNull(toSearchArgument(schema, eq("code", DataType.KEYWORD, new BytesRef("42"))));
    }

    public void testDeclaredIntegerOverPhysicalStringDeclinesPushdown() {
        TypeDescription schema = TypeDescription.createStruct().addField("s", TypeDescription.createString());
        assertNull(toSearchArgument(schema, eq("s", DataType.INTEGER, 42)));
    }

    public void testGenuineColumnsStillPushAfterGuard() {
        TypeDescription longSchema = TypeDescription.createStruct().addField("code", TypeDescription.createLong());
        assertNotNull("long over physical LONG still pushes", toSearchArgument(longSchema, lt("code", DataType.LONG, 42L)));
        TypeDescription strSchema = TypeDescription.createStruct().addField("s", TypeDescription.createString());
        assertNotNull(
            "keyword over physical STRING still pushes",
            toSearchArgument(strSchema, eq("s", DataType.KEYWORD, new BytesRef("x")))
        );
    }

    public void testMismatchedConjunctDroppedCompatibleLeafKept() {
        // Two pushed filters: keyword-over-long (dropped) and keyword-over-string (kept).
        TypeDescription schema = TypeDescription.createStruct()
            .addField("code", TypeDescription.createLong())
            .addField("name", TypeDescription.createString());
        SearchArgument sarg = toSearchArgument(
            schema,
            eq("code", DataType.KEYWORD, new BytesRef("42")),
            eq("name", DataType.KEYWORD, new BytesRef("bob"))
        );
        assertNotNull(sarg);
        String repr = sarg.toString();
        assertTrue("compatible name leaf is pushed", repr.contains("name"));
        assertFalse("mis-typed code leaf must not be pushed", repr.contains("code"));
    }

    // -------------------- Helpers --------------------

    private static TypeDescription nestedKeywordSchema() {
        return TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("action", TypeDescription.createString()));
    }

    private static TypeDescription nestedLongSchema() {
        return TypeDescription.createStruct()
            .addField("event", TypeDescription.createStruct().addField("id", TypeDescription.createLong()));
    }

    private static TypeDescription nestedActionAndIdSchema() {
        return TypeDescription.createStruct()
            .addField(
                "event",
                TypeDescription.createStruct()
                    .addField("action", TypeDescription.createString())
                    .addField("id", TypeDescription.createLong())
            );
    }

    private static SearchArgument toSearchArgument(TypeDescription schema, Expression... exprs) {
        return new OrcPushedExpressions(List.of(exprs)).toSearchArgument(schema);
    }

    private static FieldAttribute field(String name, DataType dataType) {
        return new FieldAttribute(SOURCE, name, new EsField(name, dataType, Collections.emptyMap(), true, TimeSeriesFieldType.NONE));
    }

    private static Literal literal(Object value, DataType dataType) {
        Object literalValue = value;
        if (value instanceof String s) {
            literalValue = new BytesRef(s);
        }
        return new Literal(SOURCE, literalValue, dataType);
    }

    private static Expression eq(String fieldName, DataType type, Object value) {
        return new Equals(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression neq(String fieldName, DataType type, Object value) {
        return new NotEquals(SOURCE, field(fieldName, type), literal(value, type));
    }

    private static Expression lt(String fieldName, DataType type, Object value) {
        return new LessThan(SOURCE, field(fieldName, type), literal(value, type));
    }
}
