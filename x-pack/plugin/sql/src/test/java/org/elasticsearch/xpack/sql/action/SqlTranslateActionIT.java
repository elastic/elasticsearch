/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortBuilders;

import static java.util.Collections.singletonList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class SqlTranslateActionIT extends AbstractSqlIntegTestCase {

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/37191")
    public void testSqlTranslateAction() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareBulk()
                .add(new IndexRequest("test", "doc", "1").source("data", "bar", "count", 42))
                .add(new IndexRequest("test", "doc", "2").source("data", "baz", "count", 43))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        ensureYellow("test");

        boolean columnOrder = randomBoolean();
        String columns = columnOrder ? "data, count" : "count, data";
        SqlTranslateResponse response = new SqlTranslateRequestBuilder(client(), SqlTranslateAction.INSTANCE)
                .query("SELECT " + columns + " FROM test ORDER BY count").get();
        SearchSourceBuilder source = response.source();
        FetchSourceContext fetch = source.fetchSource();
        assertEquals(true, fetch.fetchSource());
        assertArrayEquals(new String[] { "data" }, fetch.includes());
        assertEquals(
                singletonList(new DocValueFieldsContext.FieldAndFormat("count", DocValueFieldsContext.USE_DEFAULT_FORMAT)),
                source.docValueFields());
        assertEquals(singletonList(SortBuilders.fieldSort("count")), source.sorts());
    }
}
