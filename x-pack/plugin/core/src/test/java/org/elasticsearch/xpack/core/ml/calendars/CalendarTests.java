/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.calendars;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CalendarTests extends AbstractXContentSerializingTestCase<Calendar> {

    public static Calendar testInstance() {
        return testInstance(JobTests.randomValidJobId());
    }

    public static Calendar testInstance(String calendarId) {
        int size = randomInt(10);
        List<String> items = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            items.add(randomAlphaOfLengthBetween(1, 20));
        }
        String description = null;
        if (randomBoolean()) {
            description = randomAlphaOfLength(20);
        }
        return new Calendar(calendarId, items, description);
    }

    @Override
    protected Calendar createTestInstance() {
        return testInstance();
    }

    @Override
    protected Calendar mutateInstance(Calendar instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Calendar> instanceReader() {
        return Calendar::new;
    }

    @Override
    protected Calendar doParseInstance(XContentParser parser) throws IOException {
        return Calendar.STRICT_PARSER.apply(parser, null).build();
    }

    public void testNullId() {
        NullPointerException ex = expectThrows(NullPointerException.class, () -> new Calendar(null, Collections.emptyList(), null));
        assertEquals(Calendar.ID.getPreferredName() + " must not be null", ex.getMessage());
    }

    public void testDocumentId() {
        assertThat(Calendar.documentId("foo"), equalTo("calendar_foo"));
    }

    public void testStrictParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> Calendar.STRICT_PARSER.apply(parser, null));

            assertThat(e.getMessage(), containsString("unknown field [foo]"));
        }
    }

    public void testLenientParser() throws IOException {
        String json = "{\"foo\":\"bar\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            Calendar.LENIENT_PARSER.apply(parser, null);
        }
    }
}
