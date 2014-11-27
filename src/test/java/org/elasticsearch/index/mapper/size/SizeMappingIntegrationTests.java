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
package org.elasticsearch.index.mapper.size;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

public class SizeMappingIntegrationTests extends ElasticsearchIntegrationTest {

    @Test // issue 5053
    public void testThatUpdatingMappingShouldNotRemoveSizeMappingConfiguration() throws Exception {
        String index = "foo";
        String type = "mytype";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).addMapping(type, builder));

        // check mapping again
        assertSizeMappingEnabled(index, type, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("properties").startObject("otherField").field("type", "string").endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setType(type).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, type, true);
    }

    @Test
    public void testThatSizeCanBeSwitchedOnAndOff() throws Exception {
        String index = "foo";
        String type = "mytype";

        XContentBuilder builder = jsonBuilder().startObject().startObject("_size").field("enabled", true).endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate(index).addMapping(type, builder));

        // check mapping again
        assertSizeMappingEnabled(index, type, true);

        // update some field in the mapping
        XContentBuilder updateMappingBuilder = jsonBuilder().startObject().startObject("_size").field("enabled", false).endObject().endObject();
        PutMappingResponse putMappingResponse = client().admin().indices().preparePutMapping(index).setType(type).setSource(updateMappingBuilder).get();
        assertAcked(putMappingResponse);

        // make sure size field is still in mapping
        assertSizeMappingEnabled(index, type, false);
    }

    private void assertSizeMappingEnabled(String index, String type, boolean enabled) throws IOException {
        String errMsg = String.format(Locale.ROOT, "Expected size field mapping to be " + (enabled ? "enabled" : "disabled") + " for %s/%s", index, type);
        GetMappingsResponse getMappingsResponse = client().admin().indices().prepareGetMappings(index).addTypes(type).get();
        Map<String, Object> mappingSource = getMappingsResponse.getMappings().get(index).get(type).getSourceAsMap();
        assertThat(errMsg, mappingSource, hasKey("_size"));
        String sizeAsString = mappingSource.get("_size").toString();
        assertThat(sizeAsString, is(notNullValue()));
        assertThat(errMsg, sizeAsString, is("{enabled=" + (enabled) + "}"));
    }
}
