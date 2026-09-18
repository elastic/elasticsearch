/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomProperties;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomTextEsField;

public class TextEsFieldTests extends AbstractEsFieldTypeTests<TextEsField> {
    /** Older peers omit the analyzer name while retaining the rest of the field definition. */
    public void testAnalyzerNameSerialization() throws IOException {
        var field = new TextEsField("title", Map.of(), false, false, EsField.TimeSeriesFieldType.NONE, "english");
        var oldVersion = TransportVersionUtils.getPreviousVersion(TextEsField.FIELD_CAPS_INDEX_ANALYZER);
        assertEquals(new TextEsField("title", Map.of(), false, false, EsField.TimeSeriesFieldType.NONE), copyInstance(field, oldVersion));
        assertEquals(field, copyInstance(field, TextEsField.FIELD_CAPS_INDEX_ANALYZER));
    }

    @Override
    protected TextEsField createTestInstance() {
        return randomTextEsField(4);
    }

    @Override
    protected TextEsField mutateInstance(TextEsField instance) {
        String name = instance.getName();
        Map<String, EsField> properties = instance.getProperties();
        boolean hasDocValues = instance.isAggregatable();
        boolean isAlias = instance.isAlias();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        String analyzerName = instance.analyzerName();
        switch (between(0, 5)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> hasDocValues = false == hasDocValues;
            case 3 -> isAlias = false == isAlias;
            case 4 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            case 5 -> analyzerName = randomValueOtherThan(analyzerName, () -> randomBoolean() ? null : randomAlphaOfLength(6));
            default -> throw new IllegalArgumentException();
        }
        return new TextEsField(name, properties, hasDocValues, isAlias, tsType, analyzerName);
    }
}
