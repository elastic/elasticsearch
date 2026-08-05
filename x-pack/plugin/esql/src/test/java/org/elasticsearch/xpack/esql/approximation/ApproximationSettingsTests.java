/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.approximation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ApproximationSettingsTests extends ESTestCase {

    public void testParseBooleanTrue() {
        ApproximationSettings settings = ApproximationSettings.parse(Literal.TRUE);
        assertThat(settings, equalTo(ApproximationSettings.DEFAULT));
        assertThat(settings.rows(), nullValue());
        assertThat(settings.confidenceLevel(), equalTo(0.90));
    }

    public void testParseBooleanFalse() {
        ApproximationSettings settings = ApproximationSettings.parse(Literal.FALSE);
        assertThat(settings, equalTo(ApproximationSettings.EXPLICIT_NULL));
    }

    public void testParseNull() {
        ApproximationSettings settings = ApproximationSettings.parse(Literal.NULL);
        assertThat(settings, equalTo(ApproximationSettings.EXPLICIT_NULL));
    }

    public void testParseMapWithRows() {
        ApproximationSettings settings = ApproximationSettings.parse(mapExpression("rows", 50000));
        assertThat(settings.rows(), equalTo(50000));
        assertThat(settings.confidenceLevel(), equalTo(0.90));
    }

    public void testParseMapWithConfidenceLevel() {
        ApproximationSettings settings = ApproximationSettings.parse(mapExpression("confidence_level", 0.85));
        assertThat(settings.rows(), nullValue());
        assertThat(settings.confidenceLevel(), equalTo(0.85));
    }

    public void testParseMapWithBothFields() {
        ApproximationSettings settings = ApproximationSettings.parse(mapExpression("rows", 100000, "confidence_level", 0.75));
        assertThat(settings.rows(), equalTo(100000));
        assertThat(settings.confidenceLevel(), equalTo(0.75));
    }

    public void testParseMapRowsTooSmall() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ApproximationSettings.parse(mapExpression("rows", 5000))
        );
        assertThat(e.getMessage(), containsString("[rows] must be at least 10000"));
    }

    public void testParseMapConfidenceLevelTooLow() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ApproximationSettings.parse(mapExpression("confidence_level", 0.3))
        );
        assertThat(e.getMessage(), containsString("[confidence_level] must be between 0.5 and 0.95"));
    }

    public void testParseMapConfidenceLevelTooHigh() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ApproximationSettings.parse(mapExpression("confidence_level", 0.99))
        );
        assertThat(e.getMessage(), containsString("[confidence_level] must be between 0.5 and 0.95"));
    }

    public void testParseMapUnknownKey() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ApproximationSettings.parse(mapExpression("unknown_key", 42))
        );
        assertThat(e.getMessage(), containsString("unknown key [unknown_key]"));
    }

    public void testParseInvalidExpression() {
        expectThrows(IllegalArgumentException.class, () -> ApproximationSettings.parse(Literal.keyword(Source.EMPTY, "not_valid")));
    }

    public void testMergeNull() {
        ApproximationSettings base = new ApproximationSettings(20000, 0.85);
        ApproximationSettings result = new ApproximationSettings.Builder(true).merge(base).merge(null).build();
        assertThat(result.rows(), equalTo(20000));
        assertThat(result.confidenceLevel(), equalTo(0.85));
    }

    public void testMergeOverridesRows() {
        ApproximationSettings base = new ApproximationSettings(20000, 0.85);
        ApproximationSettings override = new ApproximationSettings(50000, null);
        ApproximationSettings result = new ApproximationSettings.Builder(true).merge(base).merge(override).build();
        assertThat(result.rows(), equalTo(50000));
        assertThat(result.confidenceLevel(), equalTo(0.85));
    }

    public void testMergeOverridesConfidenceLevel() {
        ApproximationSettings base = new ApproximationSettings(20000, 0.85);
        ApproximationSettings override = new ApproximationSettings(null, 0.75);
        ApproximationSettings result = new ApproximationSettings.Builder(true).merge(base).merge(override).build();
        assertThat(result.rows(), equalTo(20000));
        assertThat(result.confidenceLevel(), equalTo(0.75));
    }

    public void testMergeOverridesBothFields() {
        ApproximationSettings base = new ApproximationSettings(20000, 0.85);
        ApproximationSettings override = new ApproximationSettings(50000, 0.75);
        ApproximationSettings result = new ApproximationSettings.Builder(true).merge(base).merge(override).build();
        assertThat(result.rows(), equalTo(50000));
        assertThat(result.confidenceLevel(), equalTo(0.75));
    }

    public void testMergeExplicitNullDisables() {
        ApproximationSettings base = new ApproximationSettings(20000, 0.85);
        ApproximationSettings result = new ApproximationSettings.Builder(true).merge(base)
            .merge(ApproximationSettings.EXPLICIT_NULL)
            .build();
        assertThat(result, nullValue());
    }

    public void testMergeExplicitNullThenReEnable() {
        ApproximationSettings result = new ApproximationSettings.Builder(true).merge(new ApproximationSettings(20000, 0.85))
            .merge(ApproximationSettings.EXPLICIT_NULL)
            .merge(ApproximationSettings.DEFAULT)
            .build();
        assertNotNull(result);
        assertThat(result.rows(), nullValue());
        assertThat(result.confidenceLevel(), equalTo(0.90));
    }

    public void testMergeDefaultPreservesNullRows() {
        ApproximationSettings result = new ApproximationSettings.Builder(true).merge(ApproximationSettings.DEFAULT).build();
        assertNotNull(result);
        assertThat(result.rows(), nullValue());
        assertThat(result.confidenceLevel(), equalTo(0.90));
    }

    private static MapExpression mapExpression(Object... keyValues) {
        assert keyValues.length % 2 == 0;
        List<Expression> entries = new ArrayList<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            entries.add(new Literal(Source.EMPTY, new BytesRef((String) keyValues[i]), DataType.KEYWORD));
            entries.add(toLiteral(keyValues[i + 1]));
        }
        return new MapExpression(Source.EMPTY, entries);
    }

    private static Literal toLiteral(Object value) {
        if (value instanceof Integer i) {
            return new Literal(Source.EMPTY, i, DataType.INTEGER);
        } else if (value instanceof Double d) {
            return new Literal(Source.EMPTY, d, DataType.DOUBLE);
        } else {
            throw new IllegalArgumentException("Unsupported literal type: " + value.getClass());
        }
    }
}
