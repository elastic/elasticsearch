/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField.DOCUMENT_ID_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.DocumentAnalyticsEventField.DOCUMENT_INDEX_FIELD;
import static org.mockito.Mockito.mock;

public class DocumentAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<String> {
    public void testParsingWhenDocumentIdIsBlank() throws IOException {
        Map<String, String> jsonMap = createTestInstance();
        jsonMap.put(DOCUMENT_ID_FIELD.getPreferredName(), " ");

        BytesReference json = convertMapToJson(jsonMap);

        try (XContentParser xContentParser = createXContentParser(json)) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> parser().parse(xContentParser, mock(AnalyticsEvent.Context.class))
            );
            assertTrue(e.getMessage().contains("[document] failed to parse field [id]"));
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains("field [id] can't be blank"));
        }
    }

    @Override
    public List<String> requiredFields() {
        return Collections.singletonList(DOCUMENT_ID_FIELD.getPreferredName());
    }

    @Override
    protected Map<String, String> createTestInstance() {
        return new HashMap<>(randomEventDocumentField());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, String>> parser() {
        return DocumentAnalyticsEventField::fromXContent;
    }

    public static Map<String, String> randomEventDocumentField() {
        return Map.of(
            DOCUMENT_ID_FIELD.getPreferredName(),
            randomIdentifier(),
            DOCUMENT_INDEX_FIELD.getPreferredName(),
            randomIdentifier()
        );
    }
}
