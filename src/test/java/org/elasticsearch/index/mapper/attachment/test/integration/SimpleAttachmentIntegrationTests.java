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

package org.elasticsearch.index.mapper.attachment.test.integration;

import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.highlight.HighlightField;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.containsString;

/**
 *
 */
public class SimpleAttachmentIntegrationTests extends AttachmentIntegrationTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugins." + PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                .build();
    }

    @Before
    public void createEmptyIndex() throws Exception {
        logger.info("creating index [test]");
        internalCluster().wipeIndices("test");
        createIndex("test");
    }

    @Test
    public void testSimpleAttachment() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/test-mapping.json");
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testXHTML.html");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file", html).endObject());
        refresh();

        CountResponse countResponse = client().prepareCount("test").setQuery(queryStringQuery("test document").defaultField("file.title")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));

        countResponse = client().prepareCount("test").setQuery(queryStringQuery("tests the ability").defaultField("file")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));
    }

    @Test
    public void testSimpleAttachmentContentLengthLimit() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/test-mapping.json");
        byte[] txt = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testContentLength.txt");
        final int CONTENT_LENGTH_LIMIT = 20;

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file").startObject().field("_content", txt).field("_indexed_chars", CONTENT_LENGTH_LIMIT).endObject());
        refresh();

        CountResponse countResponse = client().prepareCount("test").setQuery(queryStringQuery("BeforeLimit").defaultField("file")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));

        countResponse = client().prepareCount("test").setQuery(queryStringQuery("AfterLimit").defaultField("file")).execute().get();
        assertThat(countResponse.getCount(), equalTo(0l));
    }

    @Test
    public void testSimpleAttachmentNoContentLengthLimit() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/test-mapping.json");
        byte[] txt = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testContentLength.txt");
        final int CONTENT_LENGTH_LIMIT = -1;

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file").startObject().field("_content", txt).field("_indexed_chars", CONTENT_LENGTH_LIMIT).endObject());
        refresh();

        CountResponse countResponse = client().prepareCount("test").setQuery(queryStringQuery("Begin").defaultField("file")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));

        countResponse = client().prepareCount("test").setQuery(queryStringQuery("End").defaultField("file")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));
    }

    /**
     * Test case for issue https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/23
     * <br/>We throw a nicer exception when no content is provided
     * @throws Exception
     */
    @Test(expected = MapperParsingException.class)
    public void testNoContent() throws Exception {
       String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/test-mapping.json");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file").startObject().endObject());
    }

    @Test
    public void testContentTypeAndName() throws Exception {
        String dummyContentType = "text/my-dummy-content-type";
        String dummyName = "my-dummy-name-txt";
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/test-mapping-store-content-type.json");
        byte[] txt = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testContentLength.txt");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file").startObject().field("_content", txt)
                .field("_content_type", dummyContentType)
                .field("_name", dummyName)
                .endObject());
        refresh();

        SearchResponse response = client().prepareSearch("test")
                .addField("file.content_type")
                .addField("file.name")
                .execute().get();

        logger.info("{}", response);

        assertThat(response.getHits().totalHits(), is(1L));
        if (assertThatWithError(response.getHits().getAt(0).getFields().get("file.content_type"), notNullValue())) {
            String contentType = response.getHits().getAt(0).getFields().get("file.content_type").getValue();
            assertThat(contentType, is(dummyContentType));
        }
        if (assertThatWithError(response.getHits().getAt(0).getFields().get("file.name"), notNullValue())) {
            String name = response.getHits().getAt(0).getFields().get("file.name").getValue();
            assertThat(name, is(dummyName));
        }
    }

    /**
     * As for now, we don't support a global `copy_to` property for `attachment` type.
     * So this test is failing.
     */
    @Test @Ignore
    public void testCopyTo() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/copy-to.json");
        byte[] txt = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/text-in-english.txt");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file", txt).endObject());
        refresh();

        CountResponse countResponse = client().prepareCount("test").setQuery(queryStringQuery("Queen").defaultField("file")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));

        countResponse = client().prepareCount("test").setQuery(queryStringQuery("Queen").defaultField("copy")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));
    }

    @Test
    public void testCopyToSubField() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/copy-to-subfield.json");
        byte[] txt = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/text-in-english.txt");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file", txt).endObject());
        refresh();

        CountResponse countResponse = client().prepareCount("test").setQuery(queryStringQuery("Queen").defaultField("file")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));

        countResponse = client().prepareCount("test").setQuery(queryStringQuery("Queen").defaultField("copy")).execute().get();
        assertThatWithError(countResponse.getCount(), equalTo(1l));
    }

    @Test
    public void testHighlightAttachment() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/attachment/test/integration/simple/test-highlight-mapping.json");
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/attachment/test/sample-files/testXHTML.html");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file", html).endObject());
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("file", "apache tika"))
                .addHighlightedField("file")
                .setNoFields().get();

        logger.info("{}", searchResponse);
        if (assertThatWithError(searchResponse.getHits().getTotalHits(), equalTo(1l))) {
            assertThat(searchResponse.getHits().getAt(0).getHighlightFields(), notNullValue());
            assertThat(searchResponse.getHits().getAt(0).getHighlightFields().keySet(), contains("file"));
            searchResponse.getHits().getAt(0).getHighlightFields();
            for (HighlightField highlightField : searchResponse.getHits().getAt(0).getHighlightFields().values()) {
                for (Text fragment : highlightField.getFragments()) {
                    assertThat(fragment.string(), containsString("<em>Apache</em>"));
                    assertThat(fragment.string(), containsString("<em>Tika</em>"));
                }
            }
        }
    }
}
