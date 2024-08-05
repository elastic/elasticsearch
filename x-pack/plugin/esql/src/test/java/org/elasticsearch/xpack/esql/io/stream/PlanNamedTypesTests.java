/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.esql.core.index.EsIndex;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Grok;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Lookup;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.DissectExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EsSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.GrokExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.LocalSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.hamcrest.Matchers.equalTo;

public class PlanNamedTypesTests extends ESTestCase {

    // List of known serializable physical plan nodes - this should be kept up to date or retrieved
    // programmatically. Excludes LocalSourceExec
    public static final List<Class<? extends PhysicalPlan>> PHYSICAL_PLAN_NODE_CLS = List.of(
        AggregateExec.class,
        DissectExec.class,
        EsQueryExec.class,
        EsSourceExec.class,
        EvalExec.class,
        EnrichExec.class,
        ExchangeExec.class,
        ExchangeSinkExec.class,
        ExchangeSourceExec.class,
        FieldExtractExec.class,
        FilterExec.class,
        FragmentExec.class,
        GrokExec.class,
        LimitExec.class,
        LocalSourceExec.class,
        HashJoinExec.class,
        MvExpandExec.class,
        OrderExec.class,
        ProjectExec.class,
        RowExec.class,
        ShowExec.class,
        TopNExec.class
    );

    // Tests that all physical plan nodes have a suitably named serialization entry.
    public void testPhysicalPlanEntries() {
        var expected = PHYSICAL_PLAN_NODE_CLS.stream().map(Class::getSimpleName).toList();
        var actual = PlanNamedTypes.namedTypeEntries()
            .stream()
            .filter(e -> e.categoryClass().isAssignableFrom(PhysicalPlan.class))
            .map(PlanNameRegistry.Entry::name)
            .toList();
        assertMap(actual, matchesList(expected));
    }

    // List of known serializable logical plan nodes - this should be kept up to date or retrieved
    // programmatically.
    public static final List<Class<? extends LogicalPlan>> LOGICAL_PLAN_NODE_CLS = List.of(
        Aggregate.class,
        Dissect.class,
        Enrich.class,
        EsRelation.class,
        EsqlProject.class,
        Eval.class,
        Filter.class,
        Grok.class,
        InlineStats.class,
        Join.class,
        Limit.class,
        LocalRelation.class,
        Lookup.class,
        MvExpand.class,
        OrderBy.class,
        Project.class,
        TopN.class
    );

    // Tests that all logical plan nodes have a suitably named serialization entry.
    public void testLogicalPlanEntries() {
        var expected = LOGICAL_PLAN_NODE_CLS.stream().map(Class::getSimpleName).toList();
        var actual = PlanNamedTypes.namedTypeEntries()
            .stream()
            .filter(e -> e.categoryClass().isAssignableFrom(LogicalPlan.class))
            .map(PlanNameRegistry.Entry::name)
            .sorted()
            .toList();
        assertMap(actual, matchesList(expected));
    }

    // Tests that all names are unique - there should be a good reason if this is not the case.
    public void testUniqueNames() {
        var actual = PlanNamedTypes.namedTypeEntries().stream().map(PlanNameRegistry.Entry::name).distinct().toList();
        assertThat(actual.size(), equalTo(PlanNamedTypes.namedTypeEntries().size()));
    }

    // Tests that reader from the original(outer) stream and inner(plan) streams work together.
    public void testWrappedStreamSimple() throws IOException {
        // write
        BytesStreamOutput bso = new BytesStreamOutput();
        bso.writeString("hello");
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry, null);
        var plan = new RowExec(Source.EMPTY, List.of(new Alias(Source.EMPTY, "foo", field("field", DataType.LONG))));
        out.writePhysicalPlanNode(plan);
        bso.writeVInt(11_345);

        // read
        StreamInput in = ByteBufferStreamInput.wrap(BytesReference.toBytes(bso.bytes()));
        assertThat(in.readString(), equalTo("hello"));
        var planStreamInput = new PlanStreamInput(in, planNameRegistry, SerializationTestUtils.writableRegistry(), EsqlTestUtils.TEST_CFG);
        var deser = (RowExec) planStreamInput.readPhysicalPlanNode();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(plan, unused -> deser);
        assertThat(in.readVInt(), equalTo(11_345));
    }

    public void testFieldSortSimple() throws IOException {
        var orig = new EsQueryExec.FieldSort(field("val", DataType.LONG), Order.OrderDirection.ASC, Order.NullsPosition.FIRST);
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry, null);
        PlanNamedTypes.writeFieldSort(out, orig);
        var deser = PlanNamedTypes.readFieldSort(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testEsIndexSimple() throws IOException {
        var orig = new EsIndex("test*", Map.of("first_name", new KeywordEsField("first_name")), Set.of("test1", "test2"));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry, null);
        PlanNamedTypes.writeEsIndex(out, orig);
        var deser = PlanNamedTypes.readEsIndex(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testDissectParserSimple() throws IOException {
        String pattern = "%{b} %{c}";
        var orig = new Dissect.Parser(pattern, ",", new DissectParser(pattern, ","));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry, null);
        PlanNamedTypes.writeDissectParser(out, orig);
        var deser = PlanNamedTypes.readDissectParser(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testEsRelation() throws IOException {
        var orig = new EsRelation(
            Source.EMPTY,
            randomEsIndex(),
            List.of(randomFieldAttribute()),
            randomFrom(IndexMode.values()),
            randomBoolean()
        );
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry, null);
        PlanNamedTypes.writeEsRelation(out, orig);
        var deser = PlanNamedTypes.readEsRelation(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testEsqlProject() throws IOException {
        var orig = new EsqlProject(
            Source.EMPTY,
            new EsRelation(Source.EMPTY, randomEsIndex(), List.of(randomFieldAttribute()), randomFrom(IndexMode.values()), randomBoolean()),
            List.of(randomFieldAttribute())
        );
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry, null);
        PlanNamedTypes.writeEsqlProject(out, orig);
        var deser = PlanNamedTypes.readEsqlProject(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testMvExpand() throws IOException {
        var esRelation = new EsRelation(
            Source.EMPTY,
            randomEsIndex(),
            List.of(randomFieldAttribute()),
            randomFrom(IndexMode.values()),
            randomBoolean()
        );
        var orig = new MvExpand(Source.EMPTY, esRelation, randomFieldAttribute(), randomFieldAttribute());
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry, null);
        PlanNamedTypes.writeMvExpand(out, orig);
        var deser = PlanNamedTypes.readMvExpand(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    private static <T> void assertNamedType(Class<T> type, T origObj) {
        var deserObj = serializeDeserialize(origObj, (o, v) -> o.writeNamed(type, origObj), i -> i.readNamed(type));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origObj, unused -> deserObj);
    }

    static EsIndex randomEsIndex() {
        Set<String> concreteIndices = new TreeSet<>();
        while (concreteIndices.size() < 2) {
            concreteIndices.add(randomAlphaOfLengthBetween(1, 25));
        }
        return new EsIndex(
            randomAlphaOfLengthBetween(1, 25),
            Map.of(randomAlphaOfLengthBetween(1, 25), randomKeywordEsField()),
            concreteIndices
        );
    }

    static FieldAttribute randomFieldAttributeOrNull() {
        return randomBoolean() ? randomFieldAttribute() : null;
    }

    static FieldAttribute randomFieldAttribute() {
        return new FieldAttribute(
            Source.EMPTY,
            randomFieldAttributeOrNull(), // parent
            randomAlphaOfLength(randomIntBetween(1, 25)), // name
            randomDataType(),
            randomEsField(),
            randomStringOrNull(), // qualifier
            randomNullability(),
            nameIdOrNull(),
            randomBoolean() // synthetic
        );
    }

    static KeywordEsField randomKeywordEsField() {
        return new KeywordEsField(
            randomAlphaOfLength(randomIntBetween(1, 25)), // name
            randomProperties(),
            randomBoolean(), // hasDocValues
            randomIntBetween(1, 12), // precision
            randomBoolean(), // normalized
            randomBoolean() // alias
        );
    }

    static EsqlBinaryComparison randomBinaryComparison() {
        int v = randomIntBetween(0, 5);
        var left = field(randomName(), randomDataType());
        var right = field(randomName(), randomDataType());
        return switch (v) {
            case 0 -> new Equals(Source.EMPTY, left, right);
            case 1 -> new NotEquals(Source.EMPTY, left, right);
            case 2 -> new GreaterThan(Source.EMPTY, left, right);
            case 3 -> new GreaterThanOrEqual(Source.EMPTY, left, right);
            case 4 -> new LessThan(Source.EMPTY, left, right);
            case 5 -> new LessThanOrEqual(Source.EMPTY, left, right);
            default -> throw new AssertionError(v);
        };
    }

    static ArithmeticOperation randomArithmeticOperation() {
        int v = randomIntBetween(0, 4);
        var left = field(randomName(), randomDataType());
        var right = field(randomName(), randomDataType());
        return switch (v) {
            case 0 -> new Add(Source.EMPTY, left, right);
            case 1 -> new Sub(Source.EMPTY, left, right);
            case 2 -> new Mul(Source.EMPTY, left, right);
            case 3 -> new Div(Source.EMPTY, left, right);
            case 4 -> new Mod(Source.EMPTY, left, right);
            default -> throw new AssertionError(v);
        };
    }

    static NameId nameIdOrNull() {
        return randomBoolean() ? new NameId() : null;
    }

    static Nullability randomNullability() {
        int i = randomInt(2);
        return switch (i) {
            case 0 -> Nullability.UNKNOWN;
            case 1 -> Nullability.TRUE;
            case 2 -> Nullability.FALSE;
            default -> throw new AssertionError(i);
        };
    }

    static EsField randomEsField() {
        return randomEsField(0);
    }

    static EsField randomEsField(int depth) {
        return new EsField(
            randomAlphaOfLength(randomIntBetween(1, 25)),
            randomDataType(),
            randomProperties(depth),
            randomBoolean(), // aggregatable
            randomBoolean() // isAlias
        );
    }

    static Map<String, EsField> randomProperties() {
        return randomProperties(0);
    }

    static Map<String, EsField> randomProperties(int depth) {
        if (depth > 2) {
            return Map.of(); // prevent infinite recursion (between EsField and properties)
        }
        depth += 1;
        int size = randomIntBetween(0, 5);
        Map<String, EsField> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(
                randomAlphaOfLength(randomIntBetween(1, 10)), // name
                randomEsField(depth)
            );
        }
        return Map.copyOf(map);
    }

    static List<DataType> DATA_TYPES = DataType.types().stream().toList();

    static DataType randomDataType() {
        return DATA_TYPES.get(randomIntBetween(0, DATA_TYPES.size() - 1));
    }

    static String randomStringOrNull() {
        return randomBoolean() ? randomAlphaOfLength(randomIntBetween(1, 25)) : null;
    }

    static String randomName() {
        return randomAlphaOfLength(randomIntBetween(1, 25));
    }

    static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Collections.emptyMap(), false));
    }

    static PlanNameRegistry planNameRegistry = new PlanNameRegistry();

    static PlanStreamInput planStreamInput(BytesStreamOutput out) {
        StreamInput in = new NamedWriteableAwareStreamInput(
            ByteBufferStreamInput.wrap(BytesReference.toBytes(out.bytes())),
            SerializationTestUtils.writableRegistry()
        );
        return new PlanStreamInput(in, planNameRegistry, SerializationTestUtils.writableRegistry(), EsqlTestUtils.TEST_CFG);
    }
}
