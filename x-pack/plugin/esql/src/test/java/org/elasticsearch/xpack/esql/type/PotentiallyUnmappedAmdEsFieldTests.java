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
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedAmdEsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PotentiallyUnmappedAmdEsFieldTests extends AbstractEsFieldTypeTests<PotentiallyUnmappedAmdEsField> {
    @Override
    protected PotentiallyUnmappedAmdEsField createTestInstance() {
        return new PotentiallyUnmappedAmdEsField(
            new EsField(
                randomAlphaOfLength(4),
                DataType.AGGREGATE_METRIC_DOUBLE,
                randomProperties(4),
                randomBoolean(),
                randomBoolean(),
                randomFrom(EsField.TimeSeriesFieldType.values())
            )
        );
    }

    @Override
    protected PotentiallyUnmappedAmdEsField mutateInstance(PotentiallyUnmappedAmdEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        if (randomBoolean()) {
            name = randomAlphaOfLength(name.length() + 1);
        } else {
            properties = randomValueOtherThan(properties, () -> randomProperties(4));
        }
        return new PotentiallyUnmappedAmdEsField(
            new EsField(
                name,
                DataType.AGGREGATE_METRIC_DOUBLE,
                properties,
                instance.isAggregatable(),
                instance.isAlias(),
                instance.getTimeSeriesFieldType()
            )
        );
    }

    public void testSerializesAsEsFieldToOldNodes() throws IOException {
        PotentiallyUnmappedAmdEsField field = new PotentiallyUnmappedAmdEsField(
            new EsField("name", DataType.AGGREGATE_METRIC_DOUBLE, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        EsField current = copy(field, TransportVersion.current());
        assertThat(current, instanceOf(PotentiallyUnmappedAmdEsField.class));
        assertThat(current.getName(), equalTo("name"));
        assertThat(current.getDataType(), equalTo(DataType.AGGREGATE_METRIC_DOUBLE));

        TransportVersion old = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_unmapped_amd_es_field"));
        EsField oldCopy = copy(field, old);
        assertThat(oldCopy.getClass(), equalTo(EsField.class));
        assertThat(oldCopy.getName(), equalTo("name"));
        assertThat(oldCopy.getDataType(), equalTo(DataType.AGGREGATE_METRIC_DOUBLE));
    }

    private EsField copy(EsField field, TransportVersion version) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput(); var pso = new PlanStreamOutput(output, EsqlTestUtils.TEST_CFG)) {
            pso.setTransportVersion(version);
            field.writeTo(pso);
            try (
                var in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()));
                var psi = new PlanStreamInput(in, in.namedWriteableRegistry(), config(), new SerializationTestUtils.TestNameIdMapper())
            ) {
                psi.setTransportVersion(version);
                return EsField.readFrom(psi);
            }
        }
    }
}
