/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.alerts.input.search.SearchInput;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.alerts.support.AlertUtils.flattenModel;
import static org.elasticsearch.alerts.support.AlertsDateUtils.formatDate;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class AlertUtilsTests extends ElasticsearchTestCase {

    @Test
    public void testFlattenModel() throws Exception {
        DateTime now = new DateTime();
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("a", ImmutableMap.builder().put("a1", new int[] { 0, 1, 2 }).build())
                .put("b", new String[] { "b0", "b1", "b2" })
                .put("c", ImmutableList.of(TimeValue.timeValueSeconds(0), TimeValue.timeValueSeconds(1)))
                .put("d", now)
                .build();

        Map<String, String> result = flattenModel(map);
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
        Map<String, Object> result = AlertUtils.responseToData(content);
        assertThat(result, equalTo(expected));
    }

    @Test
    public void testSerializeSearchRequest() throws Exception {
        String[] randomIndices = generateRandomStringArray(5, 5);
        SearchRequest expectedRequest = new SearchRequest(randomIndices);
        expectedRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), AlertUtils.DEFAULT_INDICES_OPTIONS));
        expectedRequest.searchType(randomFrom(SearchType.values()));

        SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource().query(QueryBuilders.matchAllQuery()).size(11);
        XContentBuilder searchSourceJsonBuilder = jsonBuilder();
        searchSourceBuilder.toXContent(searchSourceJsonBuilder, ToXContent.EMPTY_PARAMS);
        String expectedSource = searchSourceJsonBuilder.string();
        expectedRequest.source(expectedSource);

        if (randomBoolean()) {
            expectedRequest.templateName(randomAsciiOfLengthBetween(1, 5));
            expectedRequest.templateType(randomFrom(ScriptService.ScriptType.values()));
            if (randomBoolean()) {
                Map<String, String> params = new HashMap<>();
                int maxParams = randomIntBetween(1, 10);
                for (int i = 0; i < maxParams; i++) {
                    params.put(randomAsciiOfLengthBetween(1, 5), randomAsciiOfLengthBetween(1, 5));
                }
                expectedRequest.templateParams(params);
            }
        }

        XContentBuilder builder = jsonBuilder();
        builder = AlertUtils.writeSearchRequest(expectedRequest, builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentHelper.createParser(builder.bytes());
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        SearchRequest result = AlertUtils.readSearchRequest(parser, SearchInput.DEFAULT_SEARCH_TYPE);

        assertThat(result.indices(), arrayContainingInAnyOrder(expectedRequest.indices()));
        assertThat(result.indicesOptions(), equalTo(expectedRequest.indicesOptions()));
        assertThat(result.searchType(), equalTo(expectedRequest.searchType()));
        assertThat(result.source().toUtf8(), equalTo(expectedSource));
        assertThat(result.templateName(), equalTo(expectedRequest.templateName()));
        assertThat(result.templateType(), equalTo(expectedRequest.templateType()));
        assertThat(result.templateParams(), equalTo(expectedRequest.templateParams()));
    }

}
