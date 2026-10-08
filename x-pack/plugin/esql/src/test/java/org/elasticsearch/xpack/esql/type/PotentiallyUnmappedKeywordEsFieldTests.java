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

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomKeywordEsField;
import static org.hamcrest.Matchers.equalTo;

public class PotentiallyUnmappedKeywordEsFieldTests extends AbstractEsFieldTypeTests<PotentiallyUnmappedKeywordEsField> {
    @Override
    protected PotentiallyUnmappedKeywordEsField createTestInstance() {
        return new PotentiallyUnmappedKeywordEsField(randomAlphaOfLength(4), randomProperties(4));
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
        return new PotentiallyUnmappedKeywordEsField(name, properties);
    }

    /**
     * A {@link PotentiallyUnmappedKeywordEsField} holds only the leaf name, but nodes predating
     * {@code esql_unmapped_keyword_leaf_name} match unmapped fields by the EsField name (the full dotted path). Verify the leaf name
     * round-trips on current nodes and is expanded to the full path for older ones.
     */
    public void testSerializesFullPathToOldNodes() throws IOException {
        PotentiallyUnmappedKeywordEsField field = new PotentiallyUnmappedKeywordEsField("name");

        assertThat(copy(field, TransportVersion.current()).getName(), equalTo("name"));

        TransportVersion old = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_unmapped_keyword_leaf_name"));
        assertThat(copy(field, old).getName(), equalTo(FULL_FIELD_NAME));

        PotentiallyUnmappedKeywordEsField multiField = new PotentiallyUnmappedKeywordEsField(
            "name",
            Map.of("raw", randomKeywordEsField(0))
        );
        PotentiallyUnmappedKeywordEsField oldVersionMultiField = copy(multiField, old);
        assertThat(oldVersionMultiField.getName(), equalTo(FULL_FIELD_NAME));
        assertThat(oldVersionMultiField.getProperties(), equalTo(multiField.getProperties()));
    }

    private PotentiallyUnmappedKeywordEsField copy(PotentiallyUnmappedKeywordEsField field, TransportVersion version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput(); var pso = new PlanStreamOutput(output, EsqlTestUtils.TEST_CFG)) {
            pso.setTransportVersion(version);
            field.writeTo(pso, FULL_FIELD_NAME);
            try (
                var in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()));
                var psi = new PlanStreamInput(in, in.namedWriteableRegistry(), config(), new SerializationTestUtils.TestNameIdMapper())
            ) {
                psi.setTransportVersion(version);
                return EsField.readFrom(psi);
            }
        }
    }

    private static final String FULL_FIELD_NAME = "foo.bar.bazz";
}
