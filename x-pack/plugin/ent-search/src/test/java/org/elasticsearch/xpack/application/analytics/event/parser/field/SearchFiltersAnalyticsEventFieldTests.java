/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.analytics.event.parser.field;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ContextParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.analytics.event.AnalyticsEvent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.application.analytics.event.AnalyticsEventTestUtils.convertMapToJson;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;

public class SearchFiltersAnalyticsEventFieldTests extends AnalyticsEventFieldParserTestCase<List<String>> {
    @Override
    public void testParsingWithInvalidField() {
        // No invalid field since we are parsing a map.
    }

    public void testParsingInvalidFiltersValues() throws IOException {

        List<Object> invalidValues = List.of(
            randomInt(),
            randomDouble(),
            randomBoolean(),
            randomMap(1, 10, () -> new Tuple<>(randomIdentifier(), randomIdentifier())),
            randomList(1, 10, ESTestCase::randomInt),
            randomList(1, 10, ESTestCase::randomBoolean)
        );

        for (Object value : invalidValues) {
            String fieldName = randomIdentifier();
            Map<String, Object> jsonMap = Map.of(fieldName, value);

            BytesReference json = convertMapToJson(jsonMap);

            try (XContentParser xContentParser = createXContentParser(json)) {
                Exception e = expectThrows(
                    IllegalArgumentException.class,
                    () -> parser().parse(xContentParser, mock(AnalyticsEvent.Context.class))
                );
                assertThat(e.getMessage(), containsString(Strings.format("[%s] must be a string or an array of string.", fieldName)));
            }
        }
    }

    @Override
    public List<String> requiredFields() {
        return Collections.emptyList();
    }

    @Override
    protected Map<String, List<String>> createTestInstance() {
        return randomEventSearchFiltersField();
    }

    @Override
    protected ContextParser<AnalyticsEvent.Context, Map<String, List<String>>> parser() {
        return SearchFiltersAnalyticsEventField::fromXContent;
    }

    public static Map<String, List<String>> randomEventSearchFiltersField() {
        return randomMap(1, 10, () -> new Tuple<>(randomIdentifier(), randomFilterValue()));
    }

    private static List<String> randomFilterValue() {
        return randomList(1, 10, ESTestCase::randomIdentifier);
    }
}
