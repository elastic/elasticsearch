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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
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

import static org.elasticsearch.test.ListMatcher.matchesList;
import static org.elasticsearch.test.MapMatcher.assertMap;
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
            randomNullability(),
            nameIdOrNull(),
            randomBoolean() // synthetic
        );
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

    public static EsField randomEsField() {
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
