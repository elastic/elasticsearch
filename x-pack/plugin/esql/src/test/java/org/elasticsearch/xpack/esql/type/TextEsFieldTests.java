/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.IndexAnalyzerGroup;
import org.elasticsearch.xpack.esql.core.type.TextEsField;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomAnalyzerGroups;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomProperties;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomTextEsField;

public class TextEsFieldTests extends AbstractEsFieldTypeTests<TextEsField> {
    /**
     * Older peers omit the analyzer name, gap, unknown-analyzer reason, and (on a conflict) which indices use which
     * analyzer, while retaining the rest of the field. All of it rides the same transport version.
     */
    public void testAnalyzerMetadataSerialization() throws IOException {
        var oldVersion = TransportVersionUtils.getPreviousVersion(TextEsField.FIELD_CAPS_INDEX_ANALYZER);
        var bareField = new TextEsField("title", Map.of(), false, false, EsField.TimeSeriesFieldType.NONE);
        record Case(String analyzerName, int gap, TextEsField.UnknownAnalyzer unknown, List<IndexAnalyzerGroup> groups) {}
        for (Case c : List.of(
            new Case("english", 0, TextEsField.UnknownAnalyzer.NONE, null),
            new Case(null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, TextEsField.UnknownAnalyzer.CONFLICT, randomAnalyzerGroups()),
            new Case(null, TextEsField.DEFAULT_POSITION_INCREMENT_GAP, TextEsField.UnknownAnalyzer.INDEX_LOCAL, null)
        )) {
            var field = new TextEsField(
                "title",
                Map.of(),
                false,
                false,
                EsField.TimeSeriesFieldType.NONE,
                c.analyzerName(),
                c.gap(),
                c.unknown(),
                c.groups()
            );
            assertEquals(c.unknown(), field.unknownAnalyzer());
            assertEquals(bareField, copyInstance(field, oldVersion));
            assertEquals(field, copyInstance(field, TextEsField.FIELD_CAPS_INDEX_ANALYZER));
        }
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
        TextEsField.UnknownAnalyzer unknownAnalyzer = instance.unknownAnalyzer();
        List<IndexAnalyzerGroup> analyzerGroups = instance.analyzerGroups();
        switch (between(0, 8)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> properties = randomValueOtherThan(properties, () -> randomProperties(4));
            case 2 -> hasDocValues = false == hasDocValues;
            case 3 -> isAlias = false == isAlias;
            case 4 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            case 5 -> {
                analyzerName = randomValueOtherThan(analyzerName, () -> randomBoolean() ? null : randomAlphaOfLength(6));
                // A known name leaves nothing unknown, so naming the analyzer clears the reason and the groups.
                if (analyzerName != null) {
                    unknownAnalyzer = TextEsField.UnknownAnalyzer.NONE;
                    analyzerGroups = null;
                }
            }
            case 6 -> {
                analyzerName = analyzerName == null ? randomAlphaOfLength(6) : analyzerName;
                unknownAnalyzer = TextEsField.UnknownAnalyzer.NONE;
                analyzerGroups = null;
                positionIncrementGap = randomValueOtherThan(positionIncrementGap, () -> randomIntBetween(0, 1000));
            }
            case 7 -> {
                // The resolver never sets both a name and a reason it is unknown; keep the invariant when changing one.
                analyzerName = null;
                positionIncrementGap = TextEsField.DEFAULT_POSITION_INCREMENT_GAP;
                unknownAnalyzer = randomValueOtherThan(unknownAnalyzer, () -> randomFrom(TextEsField.UnknownAnalyzer.values()));
                analyzerGroups = unknownAnalyzer == TextEsField.UnknownAnalyzer.CONFLICT ? analyzerGroups : null;
            }
            case 8 -> {
                // Groups only accompany a conflict.
                analyzerName = null;
                positionIncrementGap = TextEsField.DEFAULT_POSITION_INCREMENT_GAP;
                unknownAnalyzer = TextEsField.UnknownAnalyzer.CONFLICT;
                analyzerGroups = randomValueOtherThan(analyzerGroups, () -> randomBoolean() ? null : randomAnalyzerGroups());
            }
            default -> throw new IllegalArgumentException();
        }
        return new TextEsField(
            name,
            properties,
            hasDocValues,
            isAlias,
            tsType,
            analyzerName,
            positionIncrementGap,
            unknownAnalyzer,
            analyzerGroups
        );
    }
}
