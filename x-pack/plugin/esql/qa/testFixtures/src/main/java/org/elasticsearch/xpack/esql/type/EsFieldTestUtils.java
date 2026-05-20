/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.type.UnsupportedEsField;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomList;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;

/**
 * Utility class providing factory and random-instance methods for EsField subtype testing.
 * Extracted so that test fixtures in consuming modules can build random EsField instances
 * without depending on the esql test artifact.
 */
public class EsFieldTestUtils {

    private EsFieldTestUtils() {}

    /**
     * Returns a random instance of any concrete serializable {@link EsField} subtype up to the given depth.
     */
    public static EsField randomSerializableEsField(int maxDepth) {
        return switch (between(0, 4)) {
            case 0 -> randomEsField(maxDepth);
            case 1 -> randomDateEsField(maxDepth);
            case 2 -> randomKeywordEsField(maxDepth);
            case 3 -> randomTextEsField(maxDepth);
            case 4 -> randomUnsupportedEsField(maxDepth);
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Returns a random {@link EsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static EsField randomEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        org.elasticsearch.xpack.esql.core.type.DataType esDataType = randomValueOtherThanMany(
            t -> false == t.supportedVersion().supportedLocally(),
            () -> randomFrom(org.elasticsearch.xpack.esql.core.type.DataType.types())
        );
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        boolean aggregatable = randomBoolean();
        boolean isAlias = randomBoolean();
        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());
        return new EsField(name, esDataType, properties, aggregatable, isAlias, tsType);
    }

    /**
     * Returns a random {@link DateEsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static DateEsField randomDateEsField(int maxPropertiesDepth) {
        return DateEsField.dateEsField(
            randomAlphaOfLength(5),
            randomProperties(maxPropertiesDepth),
            randomBoolean(),
            randomFrom(EsField.TimeSeriesFieldType.values())
        );
    }

    /**
     * Returns a random {@link KeywordEsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static KeywordEsField randomKeywordEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        boolean hasDocValues = randomBoolean();
        int precision = org.elasticsearch.test.ESTestCase.randomInt();
        boolean normalized = randomBoolean();
        boolean isAlias = randomBoolean();
        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());
        return new KeywordEsField(name, properties, hasDocValues, precision, normalized, isAlias, tsType);
    }

    /**
     * Returns a random {@link TextEsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static TextEsField randomTextEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        boolean hasDocValues = randomBoolean();
        boolean isAlias = randomBoolean();
        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());
        return new TextEsField(name, properties, hasDocValues, isAlias, tsType);
    }

    /**
     * Returns a random {@link UnsupportedEsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static UnsupportedEsField randomUnsupportedEsField(int maxPropertiesDepth) {
        String name = randomAlphaOfLength(4);
        List<String> originalTypes = randomOriginalTypes();
        String inherited = randomBoolean() ? null : randomAlphaOfLength(5);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth);
        return new UnsupportedEsField(name, originalTypes, inherited, properties);
    }

    /**
     * Returns a random list of original type name strings as used in {@link UnsupportedEsField}.
     */
    public static List<String> randomOriginalTypes() {
        return randomBoolean() ? List.of(randomAlphaOfLength(5)) : randomList(4, 4, () -> randomAlphaOfLength(5));
    }

    /**
     * Generate sub-properties for an {@link EsField}.
     *
     * @param maxDepth the maximum number of levels of properties to make
     */
    public static Map<String, EsField> randomProperties(int maxDepth) {
        if (maxDepth < 0) {
            throw new IllegalArgumentException("depth must be >= 0");
        }
        if (maxDepth == 0 || randomBoolean()) {
            return Map.of();
        }
        int targetSize = between(1, 5);
        Map<String, EsField> properties = new TreeMap<>();
        while (properties.size() < targetSize) {
            properties.put(randomAlphaOfLength(properties.size() + 1), randomSerializableEsField(maxDepth - 1));
        }
        return properties;
    }
}
