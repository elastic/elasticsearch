/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomProperties;
import static org.hamcrest.Matchers.equalTo;

public class PotentiallyUnmappedKeywordEsFieldTests extends AbstractEsFieldTypeTests<PotentiallyUnmappedKeywordEsField> {
    @Override
    protected PotentiallyUnmappedKeywordEsField createTestInstance() {
        return randomPotentiallyUnmappedKeywordEsField();
    }

    @Override
    protected PotentiallyUnmappedKeywordEsField mutateInstance(PotentiallyUnmappedKeywordEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        if (randomBoolean()) {
            name = randomAlphaOfLength(name.length() + 1);
        } else {
            properties = randomValueOtherThan(properties, () -> randomProperties(4));
        }
        return withProperties(name, properties);
    }

    /**
     * A {@link PotentiallyUnmappedKeywordEsField} holds only the leaf name, but nodes predating
     * {@code esql_unmapped_keyword_leaf_name} match unmapped fields by the EsField name (the full dotted path). Verify the leaf name
     * round-trips on current nodes and is expanded to the full path for older ones.
     */
    public void testSerializesFullPathToOldNodes() throws IOException {
        PotentiallyUnmappedKeywordEsField field = new PotentiallyUnmappedKeywordEsField("name");

        assertThat(copyWithFullName(field, "city.name", TransportVersion.current()).getName(), equalTo("name"));

        TransportVersion old = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_unmapped_keyword_leaf_name"));
        assertThat(copyWithFullName(field, "city.name", old).getName(), equalTo("city.name"));
    }

    private static PotentiallyUnmappedKeywordEsField randomPotentiallyUnmappedKeywordEsField() {
        return withProperties(randomAlphaOfLength(4), randomProperties(4));
    }

    private static PotentiallyUnmappedKeywordEsField withProperties(String name, Map<String, EsField> properties) {
        PotentiallyUnmappedKeywordEsField field = new PotentiallyUnmappedKeywordEsField(name);
        field.getProperties().putAll(properties);
        return field;
    }

    private PotentiallyUnmappedKeywordEsField copyWithFullName(
        PotentiallyUnmappedKeywordEsField field,
        String fullName,
        TransportVersion version
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput(); var pso = new PlanStreamOutput(output, EsqlTestUtils.TEST_CFG)) {
            pso.setTransportVersion(version);
            field.writeTo(pso, fullName);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()));
                var psi = new PlanStreamInput(in, in.namedWriteableRegistry(), config(), new SerializationTestUtils.TestNameIdMapper())
            ) {
                psi.setTransportVersion(version);
                return (PotentiallyUnmappedKeywordEsField) EsField.readFrom(psi);
            }
        }
    }
}
