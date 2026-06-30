/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedSingleTypeEsField;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class PotentiallyUnmappedSingleTypeEsFieldTests extends ESTestCase {
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
}
