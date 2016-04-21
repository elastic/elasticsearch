/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support;


import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.WatcherDateTimeUtils.formatDate;
import static org.elasticsearch.watcher.support.WatcherUtils.DEFAULT_INDICES_OPTIONS;
import static org.elasticsearch.watcher.support.WatcherUtils.flattenModel;
import static org.elasticsearch.watcher.test.WatcherTestUtils.getRandomSupportedSearchType;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.input.search.ExecutableSearchInput;
import org.elasticsearch.watcher.support.clock.SystemClock;
import org.elasticsearch.watcher.support.text.TextTemplate;
import org.joda.time.DateTime;

/**
 *
 */
public class WatcherUtilsTests extends ESTestCase {
    public void testFlattenModel() throws Exception {
        DateTime now = SystemClock.INSTANCE.nowUTC();
        Map<String, Object> map = new HashMap<>();
        map.put("a", singletonMap("a1", new int[] { 0, 1, 2 }));
        map.put("b", new String[] { "b0", "b1", "b2" });
        map.put("c", Arrays.asList(TimeValue.timeValueSeconds(0), TimeValue.timeValueSeconds(1)));
        map.put("d", now);

        Map<String, Object> result = flattenModel(map);
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

    public void testSerializeSearchRequest() throws Exception {
        String[] randomIndices = generateRandomStringArray(5, 5, false);
        SearchRequest expectedRequest = new SearchRequest(randomIndices);

        if (randomBoolean()) {
            String[] randomTypes = generateRandomStringArray(2, 5, false);
            expectedRequest.types(randomTypes);
        }

        expectedRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), WatcherUtils.DEFAULT_INDICES_OPTIONS));
        expectedRequest.searchType(getRandomSupportedSearchType());

        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(11);
        expectedRequest.source(searchSourceBuilder);

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
                    new Template(text, ScriptType.INLINE, null, null, params),
                    new Template(text, ScriptType.FILE, null, null, params),
                    new Template(text, ScriptType.STORED, null, null, params)
            );
            expectedRequest.template(template);
        }

        XContentBuilder builder = jsonBuilder();
        builder = WatcherUtils.writeSearchRequest(expectedRequest, builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        IndicesQueriesRegistry registry = new IndicesQueriesRegistry();
        QueryParser<MatchAllQueryBuilder> queryParser = MatchAllQueryBuilder::fromXContent;
        registry.register(queryParser, MatchAllQueryBuilder.QUERY_NAME_FIELD);
        QueryParseContext context = new QueryParseContext(registry, parser, ParseFieldMatcher.STRICT);
        SearchRequest result = WatcherUtils.readSearchRequest(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE, context, null, null);

        assertThat(result.indices(), arrayContainingInAnyOrder(expectedRequest.indices()));
        assertThat(result.types(), arrayContainingInAnyOrder(expectedRequest.types()));
        assertThat(result.indicesOptions(), equalTo(expectedRequest.indicesOptions()));
        assertThat(result.searchType(), equalTo(expectedRequest.searchType()));
        assertThat(result.source(), equalTo(searchSourceBuilder));
        assertThat(result.template(), equalTo(expectedRequest.template()));
    }

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
            indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(),
                    randomBoolean(), WatcherUtils.DEFAULT_INDICES_OPTIONS);
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
        SearchSourceBuilder searchSourceBuilder = null;
        if (randomBoolean()) {
            searchSourceBuilder = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(11);
            XContentBuilder searchSourceJsonBuilder = jsonBuilder();
            searchSourceBuilder.toXContent(searchSourceJsonBuilder, ToXContent.EMPTY_PARAMS);
            source = searchSourceBuilder.buildAsBytes(XContentType.JSON);
            builder.rawField("body", source);
        }
        Template templateSource = null;
        if (randomBoolean()) {
            Map<String, Object> params = new HashMap<>();
            if (randomBoolean()) {
                int maxParams = randomIntBetween(1, 10);
                for (int i = 0; i < maxParams; i++) {
                    params.put(randomAsciiOfLengthBetween(1, 5), randomAsciiOfLengthBetween(1, 5));
                }
            }
            String text = randomAsciiOfLengthBetween(1, 5);
            TextTemplate template = randomFrom(
                    TextTemplate.inline(text).params(params).build(),
                    TextTemplate.file(text).params(params).build(),
                    TextTemplate.indexed(text).params(params).build()
            );
            builder.field("template", template);
            templateSource = new Template(template.getTemplate(), template.getType(), null, template.getContentType(),
                    template.getParams());
        }

        XContentParser parser = XContentHelper.createParser(builder.bytes());
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        IndicesQueriesRegistry registry = new IndicesQueriesRegistry();
        QueryParser<MatchAllQueryBuilder> queryParser = MatchAllQueryBuilder::fromXContent;
        registry.register(queryParser, MatchAllQueryBuilder.QUERY_NAME_FIELD);
        QueryParseContext context = new QueryParseContext(registry, parser, ParseFieldMatcher.STRICT);
        SearchRequest result = WatcherUtils.readSearchRequest(parser, ExecutableSearchInput.DEFAULT_SEARCH_TYPE, context, null, null);

        assertThat(result.indices(), arrayContainingInAnyOrder(indices));
        assertThat(result.types(), arrayContainingInAnyOrder(types));
        assertThat(result.indicesOptions(), equalTo(indicesOptions));
        assertThat(result.searchType(), equalTo(searchType));
        assertThat(result.source(), equalTo(searchSourceBuilder));
        assertThat(result.template(), equalTo(templateSource));
    }

}
