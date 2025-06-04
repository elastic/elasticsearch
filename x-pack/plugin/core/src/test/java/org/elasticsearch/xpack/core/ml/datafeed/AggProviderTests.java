/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ml.utils.XContentObjectTransformer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class AggProviderTests extends AbstractXContentSerializingTestCase<AggProvider> {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedWriteableRegistry(searchModule.getNamedWriteables());
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return writableRegistry();
    }

    @Override
    protected AggProvider createTestInstance() {
        return createRandomValidAggProvider();
    }

    @Override
    protected Writeable.Reader<AggProvider> instanceReader() {
        return AggProvider::fromStream;
    }

    @Override
    protected AggProvider doParseInstance(XContentParser parser) throws IOException {
        return AggProvider.fromXContent(parser, false);
    }

    public static AggProvider createRandomValidAggProvider() {
        return createRandomValidAggProvider(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
    }

    public static AggProvider createRandomValidAggProvider(String name, String field) {
        Map<String, Object> agg = Map.of(name, Map.of("avg", Map.of("field", field)));
        try {
            SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
            AggregatorFactories.Builder aggs = XContentObjectTransformer.aggregatorTransformer(
                new NamedXContentRegistry(searchModule.getNamedXContents())
            ).fromMap(agg);
            return new AggProvider(agg, aggs, null, false);
        } catch (IOException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    public void testEmptyAggMap() throws IOException {
        XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, "{}");
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> AggProvider.fromXContent(parser, false));
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Datafeed aggregations are not parsable"));
    }

    private static <K, V> HashMap<K, V> hashMapOf(K key, V value) {
        HashMap<K, V> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private static <K, V> HashMap<K, V> hashMapOf(K k1, V v1, K k2, V v2, K k3, V v3) {
        HashMap<K, V> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        map.put(k3, v3);
        return map;
    }

    public void testRewriteBadNumericInterval() {
        long numericInterval = randomNonNegativeLong();
        Map<String, Object> maxTime = Map.of("max", Map.of("field", "time"));
        Map<String, Object> numericDeprecated = hashMapOf("interval", numericInterval, "field", "foo", "aggs", Map.of("time", maxTime));
        Map<String, Object> expected = Map.of("fixed_interval", numericInterval + "ms", "field", "foo", "aggs", Map.of("time", maxTime));
        Map<String, Object> deprecated = hashMapOf("buckets", hashMapOf("date_histogram", numericDeprecated));
        assertTrue(AggProvider.rewriteDateHistogramInterval(deprecated, false));
        assertThat(deprecated, equalTo(Map.of("buckets", Map.of("date_histogram", expected))));

        numericDeprecated = hashMapOf("interval", numericInterval + "ms", "field", "foo", "aggs", Map.of("time", maxTime));
        deprecated = hashMapOf("date_histogram", hashMapOf("date_histogram", numericDeprecated));
        assertTrue(AggProvider.rewriteDateHistogramInterval(deprecated, false));
        assertThat(deprecated, equalTo(Map.of("date_histogram", Map.of("date_histogram", expected))));
    }

    public void testRewriteBadCalendarInterval() {
        String calendarInterval = "1w";
        Map<String, Object> maxTime = Map.of("max", Map.of("field", "time"));
        Map<String, Object> calendarDeprecated = hashMapOf("interval", calendarInterval, "field", "foo", "aggs", Map.of("time", maxTime));
        Map<String, Object> expected = Map.of("calendar_interval", calendarInterval, "field", "foo", "aggs", Map.of("time", maxTime));
        Map<String, Object> deprecated = hashMapOf("buckets", hashMapOf("date_histogram", calendarDeprecated));
        assertTrue(AggProvider.rewriteDateHistogramInterval(deprecated, false));
        assertThat(deprecated, equalTo(Map.of("buckets", Map.of("date_histogram", expected))));

        calendarDeprecated = hashMapOf("interval", calendarInterval, "field", "foo", "aggs", Map.of("time", maxTime));
        deprecated = hashMapOf("date_histogram", hashMapOf("date_histogram", calendarDeprecated));
        assertTrue(AggProvider.rewriteDateHistogramInterval(deprecated, false));
        assertThat(deprecated, equalTo(Map.of("date_histogram", Map.of("date_histogram", expected))));
    }

    public void testRewriteWhenNoneMustOccur() {
        String calendarInterval = "1w";
        Map<String, Object> maxTime = Map.of("max", Map.of("field", "time"));
        Map<String, Object> calendarDeprecated = Map.of(
            "calendar_interval",
            calendarInterval,
            "field",
            "foo",
            "aggs",
            Map.of("time", maxTime)
        );
        Map<String, Object> expected = Map.of("calendar_interval", calendarInterval, "field", "foo", "aggs", Map.of("time", maxTime));
        Map<String, Object> current = Map.of("buckets", Map.of("date_histogram", calendarDeprecated));
        assertFalse(AggProvider.rewriteDateHistogramInterval(current, false));
        assertThat(current, equalTo(Map.of("buckets", Map.of("date_histogram", expected))));
    }

    @Override
    protected AggProvider mutateInstance(AggProvider instance) throws IOException {
        Exception parsingException = instance.getParsingException();
        AggregatorFactories.Builder parsedAggs = instance.getParsedAggs();
        switch (between(0, 1)) {
            case 0 -> parsingException = parsingException == null ? new IOException("failed parsing") : null;
            case 1 -> parsedAggs = parsedAggs == null
                ? XContentObjectTransformer.aggregatorTransformer(xContentRegistry()).fromMap(instance.getAggs())
                : null;
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new AggProvider(instance.getAggs(), parsedAggs, parsingException, false);
    }
}
