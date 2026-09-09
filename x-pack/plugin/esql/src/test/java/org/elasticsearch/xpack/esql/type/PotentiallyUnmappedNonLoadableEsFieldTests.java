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
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedNonLoadableEsField;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PotentiallyUnmappedNonLoadableEsFieldTests extends AbstractEsFieldTypeTests<PotentiallyUnmappedNonLoadableEsField> {
    /** Types reached by this marker: mapped on a sibling branch, with no implicit conversion from KEYWORD. */
    private static DataType randomNonLoadableType() {
        return randomFrom(DataType.AGGREGATE_METRIC_DOUBLE, DataType.TEXT);
    }

    @Override
    protected PotentiallyUnmappedNonLoadableEsField createTestInstance() {
        return new PotentiallyUnmappedNonLoadableEsField(
            new EsField(
                randomAlphaOfLength(4),
                randomNonLoadableType(),
                randomProperties(4),
                randomBoolean(),
                randomBoolean(),
                randomFrom(EsField.TimeSeriesFieldType.values())
            )
        );
    }

    @Override
    protected PotentiallyUnmappedNonLoadableEsField mutateInstance(PotentiallyUnmappedNonLoadableEsField instance) {
        String name = instance.getName();
        DataType dataType = instance.getDataType();
        Map<String, EsField> properties = instance.getProperties();
        switch (between(0, 2)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> dataType = randomValueOtherThan(dataType, PotentiallyUnmappedNonLoadableEsFieldTests::randomNonLoadableType);
            case 2 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            default -> throw new AssertionError("unreachable");
        }
        return new PotentiallyUnmappedNonLoadableEsField(
            new EsField(name, dataType, properties, instance.isAggregatable(), instance.isAlias(), instance.getTimeSeriesFieldType())
        );
    }

    /** The marker carries the sibling's mapped type rather than assuming one, so serialization must round-trip it. */
    public void testSerializesAsEsFieldToOldNodes() throws IOException {
        DataType dataType = randomNonLoadableType();
        PotentiallyUnmappedNonLoadableEsField field = new PotentiallyUnmappedNonLoadableEsField(
            new EsField("name", dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        EsField current = copy(field, TransportVersion.current());
        assertThat(current, instanceOf(PotentiallyUnmappedNonLoadableEsField.class));
        assertThat(current.getName(), equalTo("name"));
        assertThat(current.getDataType(), equalTo(dataType));

        TransportVersion old = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_unmapped_non_loadable_es_field"));
        EsField oldCopy = copy(field, old);
        assertThat(oldCopy.getClass(), equalTo(EsField.class));
        assertThat(oldCopy.getName(), equalTo("name"));
        assertThat(oldCopy.getDataType(), equalTo(dataType));
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
