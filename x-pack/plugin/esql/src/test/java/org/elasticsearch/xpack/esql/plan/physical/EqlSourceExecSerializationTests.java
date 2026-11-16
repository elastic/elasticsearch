/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.MissingEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;

public class EqlSourceExecSerializationTests extends AbstractPhysicalPlanSerializationTests<EqlSourceExec> {

    public static EqlSourceExec randomEqlSourceExec() {
        Source source = randomSource();
        String query = randomAlphaOfLength(20);
        String indices = randomAlphaOfLength(8);
        Map<String, Object> options = randomOptions();
        EqlRelation.Mode mode = randomFrom(EqlRelation.Mode.values());
        List<Attribute> attributes = randomList(1, 10, EqlSourceExecSerializationTests::randomEqlAttribute);
        Integer pushedLimit = randomBoolean() ? null : between(0, 10_000);
        return new EqlSourceExec(source, query, indices, options, mode, attributes, pushedLimit, null);
    }

    /**
     * A schema attribute — the mix the EQL source can carry: a reference attribute (mapped field / synthetic), a
     * metadata attribute, or an unmapped-field column (LOAD keyword PUNK / NULLIFY MissingEsField).
     */
    private static Attribute randomEqlAttribute() {
        return switch (between(0, 3)) {
            case 0 -> randomReferenceAttribute(true);
            case 1 -> MetadataAttribute.create(randomSource(), randomFrom("_index", "_id", "_source")).toAttribute();
            case 2 -> {
                String name = randomAlphaOfLength(6);
                yield new FieldAttribute(randomSource(), name, new PotentiallyUnmappedKeywordEsField(name));
            }
            case 3 -> {
                String name = randomAlphaOfLength(6);
                yield new FieldAttribute(randomSource(), name, new MissingEsField(name));
            }
            default -> throw new IllegalStateException();
        };
    }

    private static Map<String, Object> randomOptions() {
        Map<String, Object> options = new HashMap<>();
        if (randomBoolean()) {
            options.put("size", between(1, 1000));
        }
        if (randomBoolean()) {
            options.put("event_category_field", randomAlphaOfLength(6));
        }
        return options;
    }

    @Override
    protected EqlSourceExec createTestInstance() {
        return randomEqlSourceExec();
    }

    @Override
    protected EqlSourceExec mutateInstance(EqlSourceExec instance) throws IOException {
        String query = instance.query();
        String indices = instance.indices();
        Map<String, Object> options = instance.options();
        EqlRelation.Mode mode = instance.mode();
        List<Attribute> attributes = instance.output();
        Integer pushedLimit = instance.pushedLimit();
        switch (between(0, 5)) {
            case 0 -> query = randomValueOtherThan(query, () -> randomAlphaOfLength(20));
            case 1 -> indices = randomValueOtherThan(indices, () -> randomAlphaOfLength(8));
            case 2 -> options = randomValueOtherThan(options, EqlSourceExecSerializationTests::randomOptions);
            case 3 -> mode = randomValueOtherThan(mode, () -> randomFrom(EqlRelation.Mode.values()));
            case 4 -> attributes = randomValueOtherThan(
                attributes,
                () -> randomList(1, 10, EqlSourceExecSerializationTests::randomEqlAttribute)
            );
            case 5 -> pushedLimit = randomValueOtherThan(pushedLimit, () -> randomBoolean() ? null : between(0, 10_000));
            default -> throw new IllegalStateException();
        }
        return new EqlSourceExec(instance.source(), query, indices, options, mode, attributes, pushedLimit, null);
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }

    public void testPreResolvedFieldCapsGetterAndDroppedOnSerialization() throws IOException {
        // The coordinator-resolved field-caps carrier is readable in-process but never written to the wire: a
        // deserialized copy carries null (the EQL engine simply re-resolves) and still compares equal, since the
        // carrier is excluded from equals/hashCode.
        FieldCapabilitiesResponse carrier = new FieldCapabilitiesResponse(new String[] { "logs-1" }, Map.of());
        EqlSourceExec original = new EqlSourceExec(
            Source.EMPTY,
            "process where true",
            "logs-*",
            Map.of("size", 10),
            EqlRelation.Mode.EVENT,
            List.of(randomReferenceAttribute(true)),
            5,
            carrier
        );
        assertSame(carrier, original.preResolvedFieldCaps());
        assertEquals(Integer.valueOf(5), original.pushedLimit());

        EqlSourceExec copy = copyInstance(original);
        assertNull("the transient field-caps carrier must not survive serialization", copy.preResolvedFieldCaps());
        assertEquals("dropping the redundant carrier must not change plan identity", original, copy);
    }
}
