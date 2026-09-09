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

    /**
     * The type must never write itself as a plain {@link EsField}: that would strip the instruction to fail on a {@code _source} value,
     * leaving the reader to null the column instead. It also carries the sibling's mapped type rather than assuming one.
     */
    public void testAlwaysWritesItsOwnNameAndKeepsTheMappedType() throws IOException {
        DataType dataType = randomNonLoadableType();
        PotentiallyUnmappedNonLoadableEsField field = new PotentiallyUnmappedNonLoadableEsField(
            new EsField("name", dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );

        for (TransportVersion version : List.of(TransportVersion.current(), TransportVersionUtils.randomVersion())) {
            assertThat(field.getWriteableName(version), equalTo("PotentiallyUnmappedNonLoadableEsField"));
            EsField copy = copy(field, version);
            assertThat(copy, instanceOf(PotentiallyUnmappedNonLoadableEsField.class));
            assertThat(copy.getName(), equalTo("name"));
            assertThat(copy.getDataType(), equalTo(dataType));
        }
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
