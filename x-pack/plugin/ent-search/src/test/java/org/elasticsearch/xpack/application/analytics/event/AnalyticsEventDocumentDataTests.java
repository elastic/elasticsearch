/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_ID_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventDocumentData.DOCUMENT_INDEX_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.randomEventDocumentData;

public class AnalyticsEventDocumentDataTests extends AbstractEventDataTestCase<AnalyticsEventDocumentData> {

    public void testToXContentWithOnlyRequiredFields() throws IOException {
        AnalyticsEventDocumentData document = new AnalyticsEventDocumentData(randomIdentifier());
        BytesReference json = XContentHelper.toXContent(document, XContentType.JSON, false);
        Map<String, Object> contentAsMap = XContentHelper.convertToMap(json, false, JsonXContent.jsonXContent.type()).v2();

        assertEquals(1, contentAsMap.size());

        assertTrue(contentAsMap.containsKey(DOCUMENT_ID_FIELD.getPreferredName()));
        assertEquals(document.id(), contentAsMap.get(DOCUMENT_ID_FIELD.getPreferredName()));

        assertEquals(document, parseDocumentData(json));
    }

    public void testFromXContentWhenNameIsBlank() {
        Map<String, Object> jsonMap = MapBuilder.<String, Object>newMapBuilder().put(DOCUMENT_ID_FIELD.getPreferredName(), "").map();

        Exception e = expectThrows(
            XContentParseException.class,
            LoggerMessageFormat.format("[{}] failed to parse field [{}]", DOCUMENT_FIELD.getPreferredName(), DOCUMENT_ID_FIELD),
            () -> parseDocumentData(convertMapToJson(jsonMap))
        );

        assertEquals(IllegalArgumentException.class, e.getCause().getClass());
        assertEquals(LoggerMessageFormat.format("field [{}] can't be blank", DOCUMENT_ID_FIELD), e.getCause().getMessage());
    }

    @Override
    protected void assertXContentData(AnalyticsEventDocumentData document, Map<String, Object> objectAsMap) {
        assertEquals(2, objectAsMap.size());

        assertTrue(objectAsMap.containsKey(DOCUMENT_ID_FIELD.getPreferredName()));
        assertEquals(document.id(), objectAsMap.get(DOCUMENT_ID_FIELD.getPreferredName()));

        assertTrue(objectAsMap.containsKey(DOCUMENT_INDEX_FIELD.getPreferredName()));
        assertEquals(document.index(), objectAsMap.get(DOCUMENT_INDEX_FIELD.getPreferredName()));
    }

    @Override
    protected List<String> requiredFields() {
        return Collections.singletonList(DOCUMENT_ID_FIELD.getPreferredName());
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, AnalyticsEventDocumentData> parser() {
        return AnalyticsEventDocumentData::fromXContent;
    }

    @Override
    protected Writeable.Reader<AnalyticsEventDocumentData> instanceReader() {
        return AnalyticsEventDocumentData::new;
    }

    @Override
    protected AnalyticsEventDocumentData createTestInstance() {
        return randomEventDocumentData();
    }

    @Override
    protected AnalyticsEventDocumentData mutateInstance(AnalyticsEventDocumentData instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    private static AnalyticsEventDocumentData parseDocumentData(BytesReference json) throws IOException {
        try (XContentParser contentParser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.array())) {
            return AnalyticsEventDocumentData.fromXContent(contentParser, null);
        }
    }
}
