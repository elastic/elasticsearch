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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.expression.function.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Avg;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.CountDistinct;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Max;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Median;
import org.elasticsearch.xpack.esql.expression.function.aggregate.MedianAbsoluteDeviation;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Min;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Percentile;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Sum;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.plan.logical.Dissect;
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
import org.elasticsearch.xpack.esql.plan.physical.LimitExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.OrderExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RowExec;
import org.elasticsearch.xpack.esql.plan.physical.ShowExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NameId;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.Order;
import org.elasticsearch.xpack.ql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.ArithmeticOperation;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.ql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.ql.expression.predicate.operator.comparison.NullEquals;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.DateEsField;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.ql.type.KeywordEsField;
import org.elasticsearch.xpack.ql.type.TextEsField;
import org.elasticsearch.xpack.ql.type.UnsupportedEsField;
import org.elasticsearch.xpack.ql.util.DateUtils;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.SerializationTestUtils.serializeDeserialize;
import static org.hamcrest.Matchers.equalTo;

public class PlanNamedTypesTests extends ESTestCase {

    // List of known serializable plan nodes - this should be kept up to date or retrieved
    // programmatically. Excludes LocalSourceExec
    static final List<Class<? extends PhysicalPlan>> PHYSICAL_PLAN_NODE_CLS = List.of(
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
        assertThat(actual, equalTo(expected));
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
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        var plan = new RowExec(Source.EMPTY, List.of(field("foo", DataTypes.LONG)));
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

    public void testUnsupportedAttributeSimple() throws IOException {
        var orig = new UnsupportedAttribute(
            Source.EMPTY,
            "foo",
            new UnsupportedEsField("foo", "keyword"),
            "field not supported",
            new NameId(53)
        );
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeUnsupportedAttr(out, orig);
        var deser = PlanNamedTypes.readUnsupportedAttr(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
        assertThat(deser.id(), equalTo(orig.id()));
    }

    public void testUnsupportedAttribute() {
        Stream.generate(PlanNamedTypesTests::randomUnsupportedAttribute)
            .limit(100)
            .forEach(PlanNamedTypesTests::assertNamedExpressionAndId);
    }

    public void testFieldAttributeSimple() throws IOException {
        var orig = new FieldAttribute(
            Source.EMPTY,
            null, // parent, can be null
            "bar", // name
            DataTypes.KEYWORD,
            randomEsField(),
            null, // qualifier, can be null
            Nullability.TRUE,
            new NameId(53),
            true // synthetic
        );
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeFieldAttribute(out, orig);
        var deser = PlanNamedTypes.readFieldAttribute(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
        assertThat(deser.id(), equalTo(orig.id()));
    }

    public void testFieldAttribute() {
        Stream.generate(PlanNamedTypesTests::randomFieldAttribute).limit(100).forEach(PlanNamedTypesTests::assertNamedExpressionAndId);
    }

    public void testKeywordEsFieldSimple() throws IOException {
        var orig = new KeywordEsField(
            "BarKeyField", // name
            Map.of(), // no properties
            true, // hasDocValues
            5, // precision
            true, // normalized
            true // alias
        );
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeKeywordEsField(out, orig);
        var deser = PlanNamedTypes.readKeywordEsField(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testKeywordEsField() {
        Stream.generate(PlanNamedTypesTests::randomKeywordEsField).limit(100).forEach(PlanNamedTypesTests::assertNamedEsField);
    }

    public void testTextdEsFieldSimple() throws IOException {
        var orig = new TextEsField(
            "BarKeyField", // name
            Map.of(), // no properties
            true, // hasDocValues
            true // alias
        );
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeTextEsField(out, orig);
        var deser = PlanNamedTypes.readTextEsField(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testTextEsField() {
        Stream.generate(PlanNamedTypesTests::randomTextEsField).limit(100).forEach(PlanNamedTypesTests::assertNamedEsField);
    }

    public void testEsDateFieldSimple() throws IOException {
        var orig = DateEsField.dateEsField("birth_date", Map.of(), false);
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeDateEsField(out, orig);
        var deser = PlanNamedTypes.readDateEsField(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testBinComparisonSimple() throws IOException {
        var orig = new Equals(Source.EMPTY, field("foo", DataTypes.DOUBLE), field("bar", DataTypes.DOUBLE));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        out.writeNamed(BinaryComparison.class, orig);
        var deser = (Equals) planStreamInput(bso).readNamed(BinaryComparison.class);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testBinComparison() {
        Stream.generate(PlanNamedTypesTests::randomBinaryComparison)
            .limit(100)
            .forEach(obj -> assertNamedType(BinaryComparison.class, obj));
    }

    public void testAggFunctionSimple() throws IOException {
        var orig = new Avg(Source.EMPTY, field("foo_val", DataTypes.DOUBLE));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        out.writeNamed(AggregateFunction.class, orig);
        var deser = (Avg) planStreamInput(bso).readNamed(AggregateFunction.class);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testAggFunction() {
        Stream.generate(PlanNamedTypesTests::randomAggFunction).limit(100).forEach(obj -> assertNamedType(AggregateFunction.class, obj));
    }

    public void testArithmeticOperationSimple() throws IOException {
        var orig = new Add(Source.EMPTY, field("foo", DataTypes.LONG), field("bar", DataTypes.LONG));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        out.writeNamed(ArithmeticOperation.class, orig);
        var deser = (Add) planStreamInput(bso).readNamed(ArithmeticOperation.class);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testArithmeticOperation() {
        Stream.generate(PlanNamedTypesTests::randomArithmeticOperation)
            .limit(100)
            .forEach(obj -> assertNamedType(ArithmeticOperation.class, obj));
    }

    public void testSubStringSimple() throws IOException {
        var orig = new Substring(Source.EMPTY, field("foo", DataTypes.KEYWORD), new Literal(Source.EMPTY, 1, DataTypes.INTEGER), null);
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeSubstring(out, orig);
        var deser = PlanNamedTypes.readSubstring(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testStartsWithSimple() throws IOException {
        var orig = new StartsWith(Source.EMPTY, field("foo", DataTypes.KEYWORD), new Literal(Source.EMPTY, "fo", DataTypes.KEYWORD));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeStartsWith(out, orig);
        var deser = PlanNamedTypes.readStartsWith(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testRoundSimple() throws IOException {
        var orig = new Round(Source.EMPTY, field("value", DataTypes.DOUBLE), new Literal(Source.EMPTY, 1, DataTypes.INTEGER));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeRound(out, orig);
        var deser = PlanNamedTypes.readRound(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testPowSimple() throws IOException {
        var orig = new Pow(Source.EMPTY, field("value", DataTypes.DOUBLE), new Literal(Source.EMPTY, 1, DataTypes.INTEGER));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writePow(out, orig);
        var deser = PlanNamedTypes.readPow(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testAliasSimple() throws IOException {
        var orig = new Alias(Source.EMPTY, "alias_name", field("a", DataTypes.LONG));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeAlias(out, orig);
        var deser = PlanNamedTypes.readAlias(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
        assertThat(orig.id(), equalTo(deser.id()));
    }

    public void testLiteralSimple() throws IOException {
        var orig = new Literal(Source.EMPTY, 1, DataTypes.INTEGER);
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeLiteral(out, orig);
        var deser = PlanNamedTypes.readLiteral(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testOrderSimple() throws IOException {
        var orig = new Order(Source.EMPTY, field("val", DataTypes.INTEGER), Order.OrderDirection.ASC, Order.NullsPosition.FIRST);
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeOrder(out, orig);
        var deser = PlanNamedTypes.readOrder(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testFieldSortSimple() throws IOException {
        var orig = new EsQueryExec.FieldSort(field("val", DataTypes.LONG), Order.OrderDirection.ASC, Order.NullsPosition.FIRST);
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeFieldSort(out, orig);
        var deser = PlanNamedTypes.readFieldSort(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testEsIndexSimple() throws IOException {
        var orig = new EsIndex("test*", Map.of("first_name", new KeywordEsField("first_name")), Set.of("test1", "test2"));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeEsIndex(out, orig);
        var deser = PlanNamedTypes.readEsIndex(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    public void testDissectParserSimple() throws IOException {
        String pattern = "%{b} %{c}";
        var orig = new Dissect.Parser(pattern, ",", new DissectParser(pattern, ","));
        BytesStreamOutput bso = new BytesStreamOutput();
        PlanStreamOutput out = new PlanStreamOutput(bso, planNameRegistry);
        PlanNamedTypes.writeDissectParser(out, orig);
        var deser = PlanNamedTypes.readDissectParser(planStreamInput(bso));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(orig, unused -> deser);
    }

    private static void assertNamedExpressionAndId(NamedExpression origObj) {
        var deserObj = serializeDeserialize(origObj, PlanStreamOutput::writeExpression, PlanStreamInput::readNamedExpression);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origObj, unused -> deserObj);
        assertThat(deserObj.id(), equalTo(origObj.id()));
    }

    private static <T> void assertNamedType(Class<T> type, T origObj) {
        var deserObj = serializeDeserialize(origObj, (o, v) -> o.writeNamed(type, origObj), i -> i.readNamed(type));
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origObj, unused -> deserObj);
    }

    private static void assertNamedEsField(EsField origObj) {
        var deserObj = serializeDeserialize(origObj, (o, v) -> o.writeNamed(EsField.class, v), PlanStreamInput::readEsFieldNamed);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(origObj, unused -> deserObj);
    }

    static UnsupportedAttribute randomUnsupportedAttribute() {
        return new UnsupportedAttribute(
            Source.EMPTY,
            randomAlphaOfLength(randomIntBetween(1, 25)), // name
            randomUnsupportedEsField(), // field
            randomStringOrNull(), // customMessage
            nameIdOrNull()
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

    static TextEsField randomTextEsField() {
        return new TextEsField(
            randomAlphaOfLength(randomIntBetween(1, 25)), // name
            randomProperties(),
            randomBoolean(), // hasDocValues
            randomBoolean() // alias
        );
    }

    static BinaryComparison randomBinaryComparison() {
        int v = randomIntBetween(0, 6);
        var left = field(randomName(), randomDataType());
        var right = field(randomName(), randomDataType());
        return switch (v) {
            case 0 -> new Equals(Source.EMPTY, left, right, zoneIdOrNull());
            case 1 -> new NullEquals(Source.EMPTY, left, right, zoneIdOrNull());
            case 2 -> new NotEquals(Source.EMPTY, left, right, zoneIdOrNull());
            case 3 -> new GreaterThan(Source.EMPTY, left, right, zoneIdOrNull());
            case 4 -> new GreaterThanOrEqual(Source.EMPTY, left, right, zoneIdOrNull());
            case 5 -> new LessThan(Source.EMPTY, left, right, zoneIdOrNull());
            case 6 -> new LessThanOrEqual(Source.EMPTY, left, right, zoneIdOrNull());
            default -> throw new AssertionError(v);
        };
    }

    static AggregateFunction randomAggFunction() {
        int v = randomIntBetween(0, 8);
        var field = field(randomName(), randomDataType());
        var right = field(randomName(), randomDataType());
        return switch (v) {
            case 0 -> new Avg(Source.EMPTY, field);
            case 1 -> new Count(Source.EMPTY, field);
            case 2 -> new Sum(Source.EMPTY, field);
            case 3 -> new Min(Source.EMPTY, field);
            case 4 -> new Max(Source.EMPTY, field);
            case 5 -> new Median(Source.EMPTY, field);
            case 6 -> new MedianAbsoluteDeviation(Source.EMPTY, field);
            case 7 -> new CountDistinct(Source.EMPTY, field, right);
            case 8 -> new Percentile(Source.EMPTY, field, right);
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

    static ZoneId zoneIdOrNull() {
        return randomBoolean() ? DateUtils.UTC : null;
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

    static UnsupportedEsField randomUnsupportedEsField() {
        return new UnsupportedEsField(
            randomAlphaOfLength(randomIntBetween(1, 25)), // name
            randomAlphaOfLength(randomIntBetween(1, 25)), // originalType
            randomAlphaOfLength(randomIntBetween(1, 25)), // inherited
            randomProperties()
        );
    }

    static Map<String, EsField> randomProperties() {
        return randomProperties(0);
    }

    static Map<String, EsField> randomProperties(int depth) {
        if (depth > 2) {
            return Map.of(); // prevent infinite recursion (between EsField and properties)
        }
        int size = randomIntBetween(0, 5);
        Map<String, EsField> map = new HashMap<>();
        for (int i = 0; i < size; i++) {
            map.put(
                randomAlphaOfLength(randomIntBetween(1, 10)), // name
                randomEsField(depth++)
            );
        }
        return Map.copyOf(map);
    }

    static List<DataType> DATA_TYPES = EsqlDataTypes.types().stream().toList();

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
