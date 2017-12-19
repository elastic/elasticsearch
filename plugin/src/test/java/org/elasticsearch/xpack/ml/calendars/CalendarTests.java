/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.calendars;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class CalendarTests extends AbstractSerializingTestCase<Calendar> {

    public static Calendar testInstance() {
        int size = randomInt(10);
        List<String> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        return new Calendar(randomAlphaOfLengthBetween(1, 20), items);
    }

    @Override
    protected Calendar createTestInstance() {
        return testInstance();
    }

    @Override
    protected Writeable.Reader<Calendar> instanceReader() {
        return Calendar::new;
    }

    @Override
    protected Calendar doParseInstance(XContentParser parser) throws IOException {
        return Calendar.PARSER.apply(parser, null).build();
    }

    public void testNullId() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new Calendar(null, Collections.emptyList()));
        assertEquals(Calendar.ID.getPreferredName() + " must not be null", ex.getMessage());
    }

    public void testDocumentId() {
        assertThat(Calendar.documentId("foo"), equalTo("calendar_foo"));
    }
}