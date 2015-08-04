/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.support.template.Template;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.WatcherDateTimeUtils.formatDate;
import static org.elasticsearch.watcher.support.WatcherUtils.DEFAULT_INDICES_OPTIONS;
import static org.elasticsearch.watcher.support.WatcherUtils.flattenModel;
import static org.elasticsearch.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class WatcherUtilsTests extends ESTestCase {

    @Test
    public void testFlattenModel() throws Exception {
        DateTime now = SystemClock.INSTANCE.now();
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.builder().put("a1", new int[] { 0, 1, 2 }).build())
                .put("b", new String[] { "b0", "b1", "b2" })
                .put("c", ImmutableList.of(TimeValue.timeValueSeconds(0), TimeValue.timeValueSeconds(1)))
                .put("d", now)
                .build();

        @SuppressWarnings("unchecked")
        Map<String, String> result = (Map) flattenModel(map);
        assertThat(result.size(), is(9));
        assertThat(result, hasEntry("a.a1.0", "0"));
        assertThat(result, hasEntry("a.a1.1", "1"));
        assertThat(result, hasEntry("a.a1.2", "2"));
        assertThat(result, hasEntry("b.0", "b0"));
        assertThat(result, hasEntry("b.1", "b1"));
        assertThat(result, hasEntry("b.2", "b2"));
        assertThat(result, hasEntry("c.0", "0"));
        assertThat(result, hasEntry("c.1", "1000"));
        assertThat(result, hasEntry("d", formatDate(now)));
    }

    @Test
    public void testResponseToData() throws Exception {
        final Map<String, Object> expected = new HashMap<>();
        expected.put("key1", "val");
        expected.put("key2", 1);
        expected.put("key3", 1.4);
        expected.put("key4", Arrays.asList("a", "b", "c"));
        Map<String, Object> otherMap = new HashMap<>();
        otherMap.putAll(expected);
        expected.put("key5", otherMap);
        ToXContent content = new ToXContent() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                for (Map.Entry<String, ?> entry : expected.entrySet()) {
                    builder.field(entry.getKey());
                    builder.value(entry.getValue());
                }
                return builder;
            }
        };
        Map<String, Object> result = WatcherUtils.responseToData(content);
        assertThat(result, equalTo(expected));
    }

    @Test
    public void testSerializeSearchRequest() throws Exception {
        String[] randomIndices = generateRandomStringArray(5, 5, false);
        SearchRequest expectedRequest = new SearchRequest(randomIndices);

        if (randomBoolean()) {
            String[] randomTypes = generateRandomStringArray(2, 5, false);
            expectedRequest.types(randomTypes);
        }

        expectedRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), WatcherUtils.DEFAULT_INDICES_OPTIONS));
        expectedRequest.searchType(getRandomSupportedSearchType());

        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(11);
        XContentBuilder searchSourceJsonBuilder = jsonBuilder();
        searchSourceBuilder.toXContent(searchSourceJsonBuilder, ToXContent.EMPTY_PARAMS);
        String expectedSource = searchSourceJsonBuilder.string();
        expectedRequest.source(expectedSource);

        if (randomBoolean()) {
            Map<String, Object> params = new HashMap<>();
            if (randomBoolean()) {
                int maxParams = randomIntBetween(1, 10);
                for (int i = 0; i < maxParams; i++) {
                    params.put(randomAsciiOfLengthBetween(1, 5), randomAsciiOfLengthBetween(1, 5));
                }
            }
            String text = randomAsciiOfLengthBetween(1, 5);
            Template template = randomFrom(
                    Template.inline(text).params(params).build(),
                    Template.file(text).params(params).build(),
                    Template.indexed(text).params(params).build()
            );
            expectedRequest.templateSource(jsonBuilder().startObject().field("template", template).endObject().string());
        }

        XContentBuilder builder = jsonBuilder();
        builder = WatcherUtils.writeSearchRequest(expectedRequest, builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        SearchRequest result = WatcherUtils.readSearchRequest(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE);

        assertThat(result.indices(), arrayContainingInAnyOrder(expectedRequest.indices()));
        assertThat(result.types(), arrayContainingInAnyOrder(expectedRequest.types()));
        assertThat(result.indicesOptions(), equalTo(expectedRequest.indicesOptions()));
        assertThat(result.searchType(), equalTo(expectedRequest.searchType()));
        assertThat(result.source().toUtf8(), equalTo(expectedSource));
        assertThat(result.templateSource(), equalTo(expectedRequest.templateSource()));
    }

    @Test
    public void testDeserializeSearchRequest() throws Exception {

        XContentBuilder builder = jsonBuilder().startObject();

        String[] indices = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            indices = generateRandomStringArray(5, 5, false);
            if (randomBoolean()) {
                builder.array("indices", indices);
            } else {
                builder.field("indices", Strings.arrayToCommaDelimitedString(indices));
            }
        }

        String[] types = Strings.EMPTY_ARRAY;
        if (randomBoolean()) {
            types = generateRandomStringArray(2, 5, false);
            if (randomBoolean()) {
                builder.array("types", types);
            } else {
                builder.field("types", Strings.arrayToCommaDelimitedString(types));
            }
        }

        IndicesOptions indicesOptions = DEFAULT_INDICES_OPTIONS;
        if (randomBoolean()) {
            indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), WatcherUtils.DEFAULT_INDICES_OPTIONS);
            builder.startObject("indices_options")
                    .field("allow_no_indices", indicesOptions.allowNoIndices())
                    .field("expand_wildcards", indicesOptions.expandWildcardsClosed() && indicesOptions.expandWildcardsOpen() ? "all" :
                            indicesOptions.expandWildcardsClosed() ? "closed" :
                                    indicesOptions.expandWildcardsOpen() ? "open" :
                                            "none")
                    .field("ignore_unavailable", indicesOptions.ignoreUnavailable())
                    .endObject();
        }

        SearchType searchType = SearchType.DEFAULT;
        if (randomBoolean()) {
            searchType = getRandomSupportedSearchType();
            builder.field("search_type", randomBoolean() ? searchType.name() : searchType.name().toLowerCase(Locale.ROOT));
        }

        BytesReference source = null;
        if (randomBoolean()) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(11);
            XContentBuilder searchSourceJsonBuilder = jsonBuilder();
            searchSourceBuilder.toXContent(searchSourceJsonBuilder, ToXContent.EMPTY_PARAMS);
            source = searchSourceBuilder.buildAsBytes(XContentType.JSON);
            builder.rawField("body", source);
        }

        BytesReference templateSource = null;
        if (randomBoolean()) {
            Map<String, Object> params = new HashMap<>();
            if (randomBoolean()) {
                int maxParams = randomIntBetween(1, 10);
                for (int i = 0; i < maxParams; i++) {
                    params.put(randomAsciiOfLengthBetween(1, 5), randomAsciiOfLengthBetween(1, 5));
                }
            }
            String text = randomAsciiOfLengthBetween(1, 5);
            Template template = randomFrom(
                    Template.inline(text).params(params).build(),
                    Template.file(text).params(params).build(),
                    Template.indexed(text).params(params).build()
            );
            builder.field("template", template);
            templateSource = jsonBuilder().value(template).bytes();
        }

        XContentParser parser = XContentHelper.createParser(builder.bytes());
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        SearchRequest result = WatcherUtils.readSearchRequest(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE);

        assertThat(result.indices(), arrayContainingInAnyOrder(indices));
        assertThat(result.types(), arrayContainingInAnyOrder(types));
        assertThat(result.indicesOptions(), equalTo(indicesOptions));
        assertThat(result.searchType(), equalTo(searchType));
        assertThat(result.source(), equalTo(source));
        assertThat(result.templateSource(), equalTo(templateSource));
    }

}
