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

package org.elasticsearch.plugin.mapper.attachments.test;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import static org.elasticsearch.client.Requests.putMappingRequest;
import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.*;

/**
 *
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class HighlightAttachmentIntegrationTests extends ElasticsearchIntegrationTest {

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
        createIndex("test");
    }

    @Test
    public void testHighlightAttachment() throws Exception {
        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/test-highlight-mapping.json");
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/testXHTML.html");

        client().admin().indices().putMapping(putMappingRequest("test").type("person").source(mapping)).actionGet();

        index("test", "person", jsonBuilder().startObject().field("file", html).endObject());
        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchQuery("file", "apache tika"))
                .addHighlightedField("file")
                .setNoFields().get();

        logger.info("{}", searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
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
