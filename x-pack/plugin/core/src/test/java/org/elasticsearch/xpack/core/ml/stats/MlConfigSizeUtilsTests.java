/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.stats;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class MlConfigSizeUtilsTests extends ESTestCase {

    public void testStringLength() {
        assertThat(MlConfigSizeUtils.stringLength(null), is(0L));
        assertThat(MlConfigSizeUtils.stringLength("abc"), is(3L));
    }

    public void testStringLengthShouldMeasureUtf8Bytes() {
        assertThat(MlConfigSizeUtils.stringLength("é"), is((long) "é".getBytes(StandardCharsets.UTF_8).length));
    }

    public void testStringCollectionTotalLength() {
        assertThat(MlConfigSizeUtils.stringCollectionTotalLength(null), is(0L));
        assertThat(MlConfigSizeUtils.stringCollectionTotalLength(List.of()), is(0L));
        assertThat(MlConfigSizeUtils.stringCollectionTotalLength(List.of("ab", "cde")), is(5L));
    }

    public void testStringArrayTotalLength() {
        assertThat(MlConfigSizeUtils.stringArrayTotalLength(null), is(0L));
        assertThat(MlConfigSizeUtils.stringArrayTotalLength(new String[0]), is(0L));
        assertThat(MlConfigSizeUtils.stringArrayTotalLength(new String[] { "ab", "cde" }), is(5L));
    }

    public void testMapApproxSizeBytes() {
        assertThat(MlConfigSizeUtils.mapApproxSizeBytes(null), is(0L));
        assertThat(MlConfigSizeUtils.mapApproxSizeBytes(Map.of()), is(0L));
        assertThat(MlConfigSizeUtils.mapApproxSizeBytes(Map.of("key", "value")), greaterThan(0L));
    }

    public void testToXContentApproxSizeBytes() {
        ToXContentObject object = new ToXContentObject() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field("field", "value");
                builder.endObject();
                return builder;
            }
        };
        assertThat(MlConfigSizeUtils.toXContentApproxSizeBytes(object), greaterThan(0L));
        assertThat(MlConfigSizeUtils.toXContentApproxSizeBytes(null), is(0L));
    }

    public void testToXContentApproxSizeBytesShouldReturnFailureSentinelOnIOException() {
        ToXContentObject object = new ToXContentObject() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                throw new IOException("serialization failed");
            }
        };
        assertThat(MlConfigSizeUtils.toXContentApproxSizeBytes(object), is(MlConfigSizeUtils.SIZE_MEASUREMENT_FAILURE));
    }

    public void testSumSizeBytesShouldPropagateFailure() {
        assertThat(
            MlConfigSizeUtils.sumSizeBytes(10L, MlConfigSizeUtils.SIZE_MEASUREMENT_FAILURE),
            is(MlConfigSizeUtils.SIZE_MEASUREMENT_FAILURE)
        );
        assertThat(
            MlConfigSizeUtils.sumSizeBytes(MlConfigSizeUtils.SIZE_MEASUREMENT_FAILURE, 10L),
            is(MlConfigSizeUtils.SIZE_MEASUREMENT_FAILURE)
        );
        assertThat(MlConfigSizeUtils.sumSizeBytes(10L, 5L), is(15L));
    }
}
