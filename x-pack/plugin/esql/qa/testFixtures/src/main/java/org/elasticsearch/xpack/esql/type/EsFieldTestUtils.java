/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.KeywordEsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
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
        return randomSerializableEsField(maxDepth, null);
    }

    /**
     * Version-aware variant of {@link #randomSerializableEsField(int)} that only generates fields whose
     * data types (including nested properties) are serializable on {@code supportedOn}. Pass {@code null}
     * to keep the unrestricted behavior.
     */
    public static EsField randomSerializableEsField(int maxDepth, TransportVersion supportedOn) {
        return switch (between(0, 5)) {
            case 0 -> randomEsField(maxDepth, supportedOn);
            case 1 -> randomDateEsField(maxDepth, supportedOn);
            case 2 -> randomKeywordEsField(maxDepth, supportedOn);
            case 3 -> randomTextEsField(maxDepth, supportedOn);
            case 4 -> randomPotentiallyUnmappedKeywordEsField(maxDepth, supportedOn);
            case 5 -> randomUnsupportedEsField(maxDepth, supportedOn);
            default -> throw new IllegalArgumentException();
        };
    }

    /**
     * Returns a random {@link EsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static EsField randomEsField(int maxPropertiesDepth) {
        return randomEsField(maxPropertiesDepth, null);
    }

    /**
     * Version-aware variant of {@link #randomEsField(int)}. The generated data type is restricted to
     * types that serialize on {@code supportedOn} (mirroring the gate in {@code DataType#writeTo}), or
     * unconstrained when {@code null}; under-construction types are always excluded. See
     * {@link RandomDataTypeUtils}.
     */
    public static EsField randomEsField(int maxPropertiesDepth, TransportVersion supportedOn) {
        String name = randomAlphaOfLength(4);
        DataType esDataType = RandomDataTypeUtils.randomSerializableDataType(supportedOn);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth, supportedOn);
        boolean aggregatable = randomBoolean();
        boolean isAlias = randomBoolean();
        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());
        return new EsField(name, esDataType, properties, aggregatable, isAlias, tsType);
    }

    /**
     * Returns a random {@link DateEsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static DateEsField randomDateEsField(int maxPropertiesDepth) {
        return randomDateEsField(maxPropertiesDepth, null);
    }

    /**
     * Version-aware variant of {@link #randomDateEsField(int)} that restricts nested property types to those
     * serializable on {@code supportedOn}.
     */
    public static DateEsField randomDateEsField(int maxPropertiesDepth, TransportVersion supportedOn) {
        return DateEsField.dateEsField(
            randomAlphaOfLength(5),
            randomProperties(maxPropertiesDepth, supportedOn),
            randomBoolean(),
            randomFrom(EsField.TimeSeriesFieldType.values())
        );
    }

    /**
     * Returns a random {@link KeywordEsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static KeywordEsField randomKeywordEsField(int maxPropertiesDepth) {
        return randomKeywordEsField(maxPropertiesDepth, null);
    }

    /**
     * Version-aware variant of {@link #randomKeywordEsField(int)} that restricts nested property types to those
     * serializable on {@code supportedOn}.
     */
    public static KeywordEsField randomKeywordEsField(int maxPropertiesDepth, TransportVersion supportedOn) {
        String name = randomAlphaOfLength(4);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth, supportedOn);
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
        return randomTextEsField(maxPropertiesDepth, null);
    }

    /**
     * Version-aware variant of {@link #randomTextEsField(int)} that restricts nested property types to those
     * serializable on {@code supportedOn}.
     */
    public static TextEsField randomTextEsField(int maxPropertiesDepth, TransportVersion supportedOn) {
        String name = randomAlphaOfLength(4);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth, supportedOn);
        boolean hasDocValues = randomBoolean();
        boolean isAlias = randomBoolean();
        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());
        return new TextEsField(name, properties, hasDocValues, isAlias, tsType);
    }

    public static PotentiallyUnmappedKeywordEsField randomPotentiallyUnmappedKeywordEsField(
        int maxPropertiesDepth,
        TransportVersion supportedOn
    ) {
        return new PotentiallyUnmappedKeywordEsField(randomAlphaOfLength(4), randomProperties(maxPropertiesDepth, supportedOn));
    }

    /**
     * Returns a random {@link UnsupportedEsField} instance with properties nested up to {@code maxPropertiesDepth}.
     */
    public static UnsupportedEsField randomUnsupportedEsField(int maxPropertiesDepth) {
        return randomUnsupportedEsField(maxPropertiesDepth, null);
    }

    /**
     * Version-aware variant of {@link #randomUnsupportedEsField(int)} that restricts nested property types to those
     * serializable on {@code supportedOn}.
     */
    public static UnsupportedEsField randomUnsupportedEsField(int maxPropertiesDepth, TransportVersion supportedOn) {
        String name = randomAlphaOfLength(4);
        List<String> originalTypes = randomOriginalTypes();
        String inherited = randomBoolean() ? null : randomAlphaOfLength(5);
        Map<String, EsField> properties = randomProperties(maxPropertiesDepth, supportedOn);
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
        return randomProperties(maxDepth, null);
    }

    /**
     * Version-aware variant of {@link #randomProperties(int)} that restricts generated property types to those
     * serializable on {@code supportedOn}.
     *
     * @param maxDepth the maximum number of levels of properties to make
     * @param supportedOn if non-null, only generates property types supported on this transport version
     */
    public static Map<String, EsField> randomProperties(int maxDepth, TransportVersion supportedOn) {
        if (maxDepth < 0) {
            throw new IllegalArgumentException("depth must be >= 0");
        }
        if (maxDepth == 0 || randomBoolean()) {
            return Map.of();
        }
        int targetSize = between(1, 5);
        Map<String, EsField> properties = new TreeMap<>();
        while (properties.size() < targetSize) {
            properties.put(randomAlphaOfLength(properties.size() + 1), randomSerializableEsField(maxDepth - 1, supportedOn));
        }
        return properties;
    }
}
