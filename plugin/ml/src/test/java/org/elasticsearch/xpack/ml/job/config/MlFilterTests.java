/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class MlFilterTests extends AbstractSerializingTestCase<MlFilter> {

    public static MlFilter createTestFilter() {
        return new MlFilterTests().createTestInstance();
    }

    @Override
    protected MlFilter createTestInstance() {
        int size = randomInt(10);
        List<String> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        return new MlFilter(randomAlphaOfLengthBetween(1, 20), items);
    }

    @Override
    protected Reader<MlFilter> instanceReader() {
        return MlFilter::new;
    }

    @Override
    protected MlFilter doParseInstance(XContentParser parser) {
        return MlFilter.PARSER.apply(parser, null).build();
    }

    public void testNullId() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new MlFilter(null, Collections.emptyList()));
        assertEquals(MlFilter.ID.getPreferredName() + " must not be null", ex.getMessage());
    }

    public void testNullItems() {
        NullPointerException ex =
                expectThrows(NullPointerException.class, () -> new MlFilter(randomAlphaOfLengthBetween(1, 20), null));
        assertEquals(MlFilter.ITEMS.getPreferredName() + " must not be null", ex.getMessage());
    }

    public void testDocumentId() {
        assertThat(MlFilter.documentId("foo"), equalTo("filter_foo"));
    }
}
