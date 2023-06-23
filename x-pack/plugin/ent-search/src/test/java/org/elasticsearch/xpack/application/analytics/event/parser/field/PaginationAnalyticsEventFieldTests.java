/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PaginationAnalyticsEventField.CURRENT_PAGE_FIELD;
import static org.elasticsearch.xpack.application.analytics.event.parser.field.PaginationAnalyticsEventField.PAGE_SIZE_FIELD;
import static org.mockito.Mockito.mock;

public class PaginationAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<Integer> {
    public void testParsingWhenCurrentPageIsNegative() throws IOException {
        innerTestParsingNegativeField(CURRENT_PAGE_FIELD.getPreferredName());
    }

    public void testParsingWhenPageSizeIsNegative() throws IOException {
        innerTestParsingNegativeField(PAGE_SIZE_FIELD.getPreferredName());
    }

    public void innerTestParsingNegativeField(String fieldName) throws IOException {
        Map<String, Integer> jsonMap = createTestInstance();
        jsonMap.put(fieldName, -randomNonNegativeInt());

        BytesReference json = convertMapToJson(jsonMap);

        try (XContentParser xContentParser = createXContentParser(json)) {
            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> parser().parse(xContentParser, mock(AnalyticsEvent.Context.class))
            );
            assertTrue(e.getMessage().contains(Strings.format("[page] failed to parse field [%s]", fieldName)));
            assertNotNull(e.getCause());
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getCause().getMessage().contains(Strings.format("field [%s] must be positive", fieldName)));
        }
    }

    @Override
    public List<String> requiredFields() {
        return List.of(CURRENT_PAGE_FIELD.getPreferredName(), PAGE_SIZE_FIELD.getPreferredName());
    }

    @Override
    protected Map<String, Integer> createTestInstance() {
        return randomEventSearchPaginationField();
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, Integer>> parser() {
        return PaginationAnalyticsEventField::fromXContent;
    }

    public static Map<String, Integer> randomEventSearchPaginationField() {
        return MapBuilder.<String, Integer>newMapBuilder()
            .put(CURRENT_PAGE_FIELD.getPreferredName(), randomNonNegativeInt())
            .put(PAGE_SIZE_FIELD.getPreferredName(), randomNonNegativeInt())
            .map();
    }
}
