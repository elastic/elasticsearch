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

package org.elasticsearch.search.source;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

public class SourceFetchingIT extends ESIntegTestCase {

    @Test
    public void testSourceDefaultBehavior() {
        createIndex("test");
        ensureGreen();

        index("test", "type1", "1", "field", "value");
        refresh();

        SearchResponse response = client().prepareSearch("test").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

        response = client().prepareSearch("test").addField("bla").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client().prepareSearch("test").addField("_source").get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

    }

    @Test
    public void testSourceFiltering() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field1", "value", "field2", "value2").get();
        refresh();

        SearchResponse response = client().prepareSearch("test").setFetchSource(false).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), nullValue());

        response = client().prepareSearch("test").setFetchSource(true).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());

        response = client().prepareSearch("test").setFetchSource("field1", null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSource().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSource().get("field1"), equalTo("value"));

        response = client().prepareSearch("test").setFetchSource("hello", null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSource().size(), equalTo(0));

        response = client().prepareSearch("test").setFetchSource(new String[]{"*"}, new String[]{"field2"}).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSource().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSource().get("field1"), equalTo("value"));

    }

    /**
     * Test Case for #5132: Source filtering with wildcards broken when given multiple patterns
     * https://github.com/elasticsearch/elasticsearch/issues/5132
     */
    @Test
    public void testSourceWithWildcardFiltering() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource("field", "value").get();
        refresh();

        SearchResponse response = client().prepareSearch("test").setFetchSource(new String[]{"*.notexisting","field"}, null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSource().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSource().get("field"), equalTo("value"));

        response = client().prepareSearch("test").setFetchSource(new String[]{"field.notexisting.*","field"}, null).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), notNullValue());
        assertThat(response.getHits().getAt(0).getSource().size(), equalTo(1));
        assertThat((String) response.getHits().getAt(0).getSource().get("field"), equalTo("value"));
    }
}
