/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PrometheusLabelNameUtilsTests extends ESTestCase {

    public void testDecodePlainNameReturnedAsIs() {
        assertThat(PrometheusLabelNameUtils.decodeLabelName("job"), equalTo("job"));
    }

    public void testDecodeNameWithUnderscoresReturnedAsIs() {
        assertThat(PrometheusLabelNameUtils.decodeLabelName("my_label"), equalTo("my_label"));
    }

    public void testDecodeEmptyStringReturnedAsIs() {
        assertThat(PrometheusLabelNameUtils.decodeLabelName(""), equalTo(""));
    }

    public void testDecodeNullReturnedAsNull() {
        assertThat(PrometheusLabelNameUtils.decodeLabelName(null), is(nullValue()));
    }

    public void testDecodeDoubleUnderscoreBecomesUnderscore() {
        // U____ → one underscore (U__ prefix stripped, then __ → _)
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U____"), equalTo("_"));
    }

    public void testDecodeMultipleDoubleUnderscores() {
        // U________ = U__ + ______ (6 underscores = 3 pairs of __) → ___ (three underscores)
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U________"), equalTo("___"));
    }

    public void testDecodeHexEscapeForDot() {
        // U__http_2e_status__code → http.status_code
        // _2e_ is '.' (0x2E), __ is '_'
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U__http_2e_status__code"), equalTo("http.status_code"));
    }

    public void testDecodeHexEscapeUpperCase() {
        // U___2E_ → '.' (uppercase hex)
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U___2E_"), equalTo("."));
    }

    public void testDecodeHexEscapeForColon() {
        // colon is 0x3A
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U__http_3a_requests"), equalTo("http:requests"));
    }

    public void testDecodeMultiByteCodepoint() {
        // U+1F600 = 😀 = 0x1F600
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U___1F600_"), equalTo("😀"));
    }

    public void testDecodeInvalidHexPassThrough() {
        // _xyz_ is not a valid hex sequence → pass through the underscore character
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U__abc_xyz_def"), equalTo("abc_xyz_def"));
    }

    public void testDecodeTrailingUnderscorePassThrough() {
        // Trailing _ with no closing _ → pass through
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U__abc_"), equalTo("abc_"));
    }

    public void testDecodeMixedExample() {
        // U__my__label_2e_value → my_label.value
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U__my__label_2e_value"), equalTo("my_label.value"));
    }

    public void testDecodePrefixOnlyIsEmpty() {
        // "U__" with empty body → empty string
        assertThat(PrometheusLabelNameUtils.decodeLabelName("U__"), equalTo(""));
    }
}
