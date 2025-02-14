/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.source;

import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponses;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class SourceFetchingIT extends ESIntegTestCase {

    public void testSourceDefaultBehavior() {
        createIndex("test");
        ensureGreen();

        indexDoc("test", "1", "field", "value");
        refresh();

        assertResponses(
            response -> assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue()),
            prepareSearch("test"),
            prepareSearch("test").addStoredField("_source")
        );

        assertResponse(
            prepareSearch("test").addStoredField("bla"),
            response -> assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue())
        );
    }

    public void testSourceFiltering() {
        createIndex("test");
        ensureGreen();

        prepareIndex("test").setId("1").setSource("field1", "value", "field2", "value2").get();
        refresh();

        assertResponse(
            prepareSearch("test").setFetchSource(false),
            response -> assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue())
        );

        assertResponse(
            prepareSearch("test").setFetchSource(true),
            response -> assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue())
        );

        assertResponses(response -> {
            assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
            Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
            assertThat(source.size(), equalTo(1));
            assertThat(source.get("field1"), equalTo("value"));
        },
            prepareSearch("test").setFetchSource("field1", null),
            prepareSearch("test").setFetchSource(new String[] { "*" }, new String[] { "field2" })
        );

        assertResponse(prepareSearch("test").setFetchSource("hello", null), response -> {
            assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
            assertThat(response.getHits().getAt(0).getSourceAsMap().size(), equalTo(0));
        });

    }

    /**
     * Test Case for #5132: Source filtering with wildcards broken when given multiple patterns
     * https://github.com/elastic/elasticsearch/issues/5132
     */
    public void testSourceWithWildcardFiltering() {
        createIndex("test");
        ensureGreen();

        prepareIndex("test").setId("1").setSource("field", "value").get();
        refresh();

        assertResponses(response -> {
            assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
            Map<String, Object> source = response.getHits().getAt(0).getSourceAsMap();
            assertThat(source.size(), equalTo(1));
            assertThat((String) source.get("field"), equalTo("value"));
        },
            prepareSearch("test").setFetchSource(new String[] { "*.notexisting", "field" }, null),
            prepareSearch("test").setFetchSource(new String[] { "field.notexisting.*", "field" }, null)
        );
    }
}
