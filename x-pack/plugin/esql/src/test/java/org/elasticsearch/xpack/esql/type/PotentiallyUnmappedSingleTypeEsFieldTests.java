/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PotentiallyUnmappedSingleTypeEsFieldTests extends AbstractEsFieldTypeTests<PotentiallyUnmappedSingleTypeEsField> {
    /** Regression against #150676. */
    public void testChildFieldsCanBeRegistered() {
        EsField mappedField = new EsField("parent", DataType.LONG, Map.of(), randomBoolean(), EsField.TimeSeriesFieldType.NONE);
        PotentiallyUnmappedSingleTypeEsField field = new PotentiallyUnmappedSingleTypeEsField(mappedField, Set.of("index-1"));
        assertThat(field.getProperties(), equalTo(Map.of()));

        EsField child = new EsField("keyword", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        field.getProperties().put(child.getName(), child);

        assertThat(field.getProperties(), equalTo(Map.of(child.getName(), child)));
    }

    // getDataType() widens (SHORT -> INTEGER) to present like a normal field, while types() keeps SHORT for union-type keying.
    public void testWidensSmallNumericDataType() {
        EsField mappedField = new EsField("f", DataType.SHORT, Map.of(), randomBoolean(), EsField.TimeSeriesFieldType.NONE);
        PotentiallyUnmappedSingleTypeEsField field = new PotentiallyUnmappedSingleTypeEsField(mappedField, Set.of("index-1"));

        assertThat(field.getDataType(), equalTo(DataType.INTEGER));
        assertThat(field.types(), equalTo(Set.of(DataType.SHORT)));
        assertThat(field.getTypesToIndices(), equalTo(Map.of("short", Set.of("index-1"))));
    }

    @Override
    protected PotentiallyUnmappedSingleTypeEsField createTestInstance() {
        return new PotentiallyUnmappedSingleTypeEsField(randomMappedField(), randomMappedIndices());
    }

    @Override
    protected PotentiallyUnmappedSingleTypeEsField copyInstance(PotentiallyUnmappedSingleTypeEsField instance, TransportVersion version) {
        // writeContent throws UnsupportedOperationException; copy directly without going through the wire.
        return new PotentiallyUnmappedSingleTypeEsField(
            instance.mappedField(),
            EsqlTestUtils.singleValue(instance.getTypesToIndices().values())
        );
    }

    @Override
    protected PotentiallyUnmappedSingleTypeEsField mutateInstance(PotentiallyUnmappedSingleTypeEsField instance) throws IOException {
        var mappedField = instance.mappedField();
        var mappedIndices = instance.mappedIndices();

        if (randomBoolean()) {
            mappedField = randomValueOtherThan(mappedField, PotentiallyUnmappedSingleTypeEsFieldTests::randomMappedField);
        } else {
            mappedIndices = randomValueOtherThan(mappedIndices, PotentiallyUnmappedSingleTypeEsFieldTests::randomMappedIndices);
        }
        return new PotentiallyUnmappedSingleTypeEsField(mappedField, mappedIndices);
    }

    private static EsField randomMappedField() {
        return EsFieldTestUtils.randomEsField(4);
    }

    private static Set<String> randomMappedIndices() {
        return randomSet(0, 3, () -> randomAlphaOfLength(3));
    }
}
