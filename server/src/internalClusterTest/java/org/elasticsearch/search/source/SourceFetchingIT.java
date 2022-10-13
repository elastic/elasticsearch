/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.source;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class SourceFetchingIT extends ESIntegTestCase {
    public void testSourceDefaultBehavior() {
        createIndex("test");
        ensureGreen();

        indexDoc("test", "1", "field", "value");
        refresh();

        SearchResponse response = client().prepareSearch("test").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

        response = client().prepareSearch("test").addStoredField("bla").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client().prepareSearch("test").addStoredField("_source").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

    }

    public void testSourceFiltering() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field1", "value", "field2", "value2").get();
        refresh();

        SearchResponse response = client().prepareSearch("test").setFetchSource(false).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client().prepareSearch("test").setFetchSource(true).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

        response = client().prepareSearch("test").setFetchSource("field1", null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value"));

        response = client().prepareSearch("test").setFetchSource("hello", null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(0));

        response = client().prepareSearch("test").setFetchSource(new String[] { "*" }, new String[] { "field2" }).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field1"), equalTo("value"));

    }

    /**
     * Test Case for #5132: Source filtering with wildcards broken when given multiple patterns
     * https://github.com/elastic/elasticsearch/issues/5132
     */
    public void testSourceWithWildcardFiltering() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh();

        SearchResponse response = client().prepareSearch("test").setFetchSource(new String[] { "*.notexisting", "field" }, null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("value"));

        response = client().prepareSearch("test").setFetchSource(new String[] { "field.notexisting.*", "field" }, null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSourceAsMap().get("field"), equalTo("value"));
    }
}
