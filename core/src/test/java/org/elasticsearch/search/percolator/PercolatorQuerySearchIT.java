/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.percolator;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.percolator.PercolatorFieldMapper;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.percolatorQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

public class PercolatorQuerySearchIT extends ESSingleNodeTestCase {

    public void testPercolatorQuery() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", "type=keyword", "field2", "type=keyword")
        );

        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "1")
            .setSource(jsonBuilder().startObject().field("query", matchAllQuery()).endObject())
            .get();
        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "2")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "value")).endObject())
            .get();
        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "3")
            .setSource(jsonBuilder().startObject().field("query", boolQuery()
                .must(matchQuery("field1", "value"))
                .must(matchQuery("field2", "value"))
            ).endObject()).get();
        client().admin().indices().prepareRefresh().get();

        BytesReference source = jsonBuilder().startObject().endObject().bytes();
        logger.info("percolating empty doc");
        SearchResponse response = client().prepareSearch()
            .setQuery(percolatorQuery("type", source))
            .get();
        assertHitCount(response, 1);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));

        source = jsonBuilder().startObject().field("field1", "value").endObject().bytes();
        logger.info("percolating doc with 1 field");
        response = client().prepareSearch()
            .setQuery(percolatorQuery("type", source))
            .addSort("_uid", SortOrder.ASC)
            .get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));

        source = jsonBuilder().startObject().field("field1", "value").field("field2", "value").endObject().bytes();
        logger.info("percolating doc with 2 fields");
        response = client().prepareSearch()
            .setQuery(percolatorQuery("type", source))
            .addSort("_uid", SortOrder.ASC)
            .get();
        assertHitCount(response, 3);
        assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testPercolatorQueryWithHighlighting() throws Exception {
        createIndex("test", client().admin().indices().prepareCreate("test")
                .addMapping("type", "field1", "type=text")
        );
        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "1")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "brown fox")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "2")
                .setSource(jsonBuilder().startObject().field("query", matchQuery("field1", "lazy dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "3")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "jumps")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "4")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "dog")).endObject())
                .execute().actionGet();
        client().prepareIndex("test", PercolatorFieldMapper.TYPE_NAME, "5")
                .setSource(jsonBuilder().startObject().field("query", termQuery("field1", "fox")).endObject())
                .execute().actionGet();
        client().admin().indices().prepareRefresh().get();

        BytesReference document = jsonBuilder().startObject()
                .field("field1", "The quick brown fox jumps over the lazy dog")
                .endObject().bytes();
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(percolatorQuery("type", document))
                .highlighter(new HighlightBuilder().field("field1"))
                .addSort("_uid", SortOrder.ASC)
                .get();
        assertHitCount(searchResponse, 5);

        assertThat(searchResponse.getHits().getAt(0).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick <em>brown</em> <em>fox</em> jumps over the lazy dog"));
        assertThat(searchResponse.getHits().getAt(1).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox jumps over the <em>lazy</em> <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(2).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox <em>jumps</em> over the lazy dog"));
        assertThat(searchResponse.getHits().getAt(3).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown fox jumps over the lazy <em>dog</em>"));
        assertThat(searchResponse.getHits().getAt(4).getHighlightFields().get("field1").fragments()[0].string(),
                equalTo("The quick brown <em>fox</em> jumps over the lazy dog"));;
    }

}
