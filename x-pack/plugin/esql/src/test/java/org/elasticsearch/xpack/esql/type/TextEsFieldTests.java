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
    /** Older peers omit the analyzer name, gap, and conflict flag while retaining the rest of the field. */
    public void testAnalyzerNameSerialization() throws IOException {
        var field = new TextEsField("title", Map.of(), false, false, EsField.TimeSeriesFieldType.NONE, "english", 0);
        var oldVersion = TransportVersionUtils.getPreviousVersion(TextEsField.FIELD_CAPS_INDEX_ANALYZER);
        assertEquals(new TextEsField("title", Map.of(), false, false, EsField.TimeSeriesFieldType.NONE), copyInstance(field, oldVersion));
        assertEquals(field, copyInstance(field, TextEsField.FIELD_CAPS_INDEX_ANALYZER));
    }

    /** The conflict flag rides the same transport version as the analyzer name; older peers do not see it. */
    public void testAnalyzerConflictSerialization() throws IOException {
        var conflict = new TextEsField(
            "title",
            Map.of(),
            false,
            false,
            EsField.TimeSeriesFieldType.NONE,
            null,
            TextEsField.DEFAULT_POSITION_INCREMENT_GAP,
            true
        );
        assertTrue(conflict.analyzerConflict());
        var oldVersion = TransportVersionUtils.getPreviousVersion(TextEsField.FIELD_CAPS_INDEX_ANALYZER);
        assertFalse(copyInstance(conflict, oldVersion).analyzerConflict());
        assertTrue(copyInstance(conflict, TextEsField.FIELD_CAPS_INDEX_ANALYZER).analyzerConflict());
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
        int positionIncrementGap = instance.positionIncrementGap();
        boolean analyzerConflict = instance.analyzerConflict();
        switch (between(0, 7)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> hasDocValues = false == hasDocValues;
            case 3 -> isAlias = false == isAlias;
            case 4 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            case 5 -> analyzerName = randomValueOtherThan(analyzerName, () -> randomBoolean() ? null : randomAlphaOfLength(6));
            case 6 -> {
                analyzerName = analyzerName == null ? randomAlphaOfLength(6) : analyzerName;
                positionIncrementGap = randomValueOtherThan(positionIncrementGap, () -> randomIntBetween(0, 1000));
            }
            case 7 -> {
                // The resolver never sets both an analyzer name and a conflict; keep the invariant when flipping.
                analyzerName = null;
                positionIncrementGap = TextEsField.DEFAULT_POSITION_INCREMENT_GAP;
                analyzerConflict = false == analyzerConflict;
            }
            default -> throw new IllegalArgumentException();
        }
        return new TextEsField(name, properties, hasDocValues, isAlias, tsType, analyzerName, positionIncrementGap, analyzerConflict);
    }
}
