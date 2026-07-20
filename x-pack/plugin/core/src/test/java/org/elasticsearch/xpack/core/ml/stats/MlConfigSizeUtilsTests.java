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
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class MlConfigSizeUtilsTests extends ESTestCase {

    public void testStringLength() {
        assertThat(MlConfigSizeUtils.stringLength(null), is(0L));
        assertThat(MlConfigSizeUtils.stringLength("abc"), is(3L));
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
}
