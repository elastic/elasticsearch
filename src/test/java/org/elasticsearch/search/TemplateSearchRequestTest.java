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

package org.elasticsearch.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class TemplateSearchRequestTest extends ElasticsearchIntegrationTest {

    @Before
    public void setup() {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test", "testtype").setId("1")
                .setSource("text", "value_1", "title", "value_1").get();
        client().prepareIndex("test", "testtype").setId("2")
                .setSource("text", "value_2", "title", "value_2").get();
        refresh();
    }

    @Test
    public void testTemplateInBody() {
        String request = "{\"template\":"
                + "{\"content\":"
                + "{\"query\": {\"match_{{template_1}}\": {}}," 
                + "\"fie{{template_2}}\": [\"text\", \"ti{{template_3}}\"]}," 
                + "\"params\" : {\"template_1\" : \"all\"," +
                                "\"template_2\" : \"lds\"," +
                                "\"template_3\" : \"tle\"}}}";

        SearchResponse sr = client().prepareSearch().setSource(request)
                .execute().actionGet();
        
        ElasticsearchAssertions.assertHitCount(sr, 2);
        assertEquals("Limiting fields by templates not working.", 2, sr.getHits().getAt(0).getFields().size());
        assertEquals("Limiting fields by templates not working.", 2, sr.getHits().getAt(1).getFields().size());
    }

    @Test
    public void testRawFSTemplate() throws IOException {
        String query = "{\"template\": {\"content\": \"storedTemplate\","
                + "\"params\" : {\"template_1\" : \"all\","
                + "\"template_2\" : \"lds\","
                + "\"template_3\" : \"tle\"}}}";

        SearchResponse sr = client().prepareSearch().setSource(query)
                .execute().actionGet();
        ElasticsearchAssertions.assertHitCount(sr, 2);
        assertEquals("Limiting fields by templates not working.", 2, sr.getHits().getAt(0).getFields().size());
        assertEquals("Limiting fields by templates not working.", 2, sr.getHits().getAt(1).getFields().size());
    }

    
    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        String scriptPath = this.getClass()
                .getResource("config").getPath();

        Settings settings = ImmutableSettings
                .settingsBuilder()
                .put("path.conf", scriptPath).build();

        return settings;
    }
}
