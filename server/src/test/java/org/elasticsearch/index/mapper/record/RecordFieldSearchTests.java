/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.record;

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.prefixQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCountAndNoFailures;

public class RecordFieldSearchTests extends ESSingleNodeTestCase {

    @Before
    public void setUpIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("attributes")
            .field("type", "record")
            .field("split_queries_on_whitespace", true)
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        createIndex("test", Settings.EMPTY, mapping);
    }

    public void testTermQueryOnRoot() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .field("env", "prod")
                    .field("region", "eu-west")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes", "prod")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes", "missing")), 0L);
    }

    public void testTermQueryOnSubKey() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .field("env", "prod")
                    .field("region", "eu-west")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.env", "prod")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.region", "prod")), 0L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.nonexistent", "prod")), 0L);
    }

    public void testRecursiveObjectQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .startObject("host")
                    .field("name", "node-1")
                    .field("ip", "10.0.0.1")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        // Querying a nested key via dotted path
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.host.name", "node-1")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.host.ip", "10.0.0.1")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.host.name", "node-2")), 0L);
    }

    public void testExistsQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder().startObject().startObject("attributes").field("env", "prod").endObject().endObject())
            .get();
        prepareIndex("test").setId("2")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder().startObject().field("other", "value").endObject())
            .get();

        assertHitCount(client().prepareSearch("test").setQuery(existsQuery("attributes")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(existsQuery("attributes.env")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(existsQuery("attributes.nonexistent")), 0L);
    }

    public void testPrefixQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder().startObject().startObject("attributes").field("version", "v1.2.3").endObject().endObject()
            )
            .get();

        assertHitCount(client().prepareSearch("test").setQuery(prefixQuery("attributes.version", "v1")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(prefixQuery("attributes.version", "v2")), 0L);
    }

    public void testRangeQuery() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(XContentFactory.jsonBuilder().startObject().startObject("attributes").field("priority", "5").endObject().endObject())
            .get();

        // Values are always strings; range is lexicographic
        assertHitCountAndNoFailures(client().prepareSearch("test").setQuery(rangeQuery("attributes.priority").gte("3").lte("7")), 1);
        assertHitCountAndNoFailures(client().prepareSearch("test").setQuery(rangeQuery("attributes.priority").gte("6").lte("9")), 0);
    }

    public void testScalarArrayValues() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .array("tags", "alpha", "beta", "gamma")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.tags", "alpha")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.tags", "beta")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.tags", "missing")), 0L);
    }

    public void testMixedNestedStructure() throws Exception {
        prepareIndex("test").setId("1")
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .field("env", "staging")
                    .startObject("build")
                    .field("version", "2.0")
                    .field("commit", "abc123")
                    .endObject()
                    .array("owners", "alice", "bob")
                    .endObject()
                    .endObject()
            )
            .get();

        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.env", "staging")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.build.version", "2.0")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.build.commit", "abc123")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.owners", "alice")), 1L);
        assertHitCount(client().prepareSearch("test").setQuery(termQuery("attributes.owners", "bob")), 1L);
    }
}
