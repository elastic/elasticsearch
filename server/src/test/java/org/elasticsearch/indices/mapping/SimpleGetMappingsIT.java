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

package org.elasticsearch.indices.mapping;

import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.cluster.metadata.IndexMetaData.INDEX_METADATA_BLOCK;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_METADATA;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_READ;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_BLOCKS_WRITE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_READ_ONLY;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

public class SimpleGetMappingsIT extends ESIntegTestCase {

    public void testGetMappingsWhereThereAreNone() {
        createIndex("index");
        GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
        assertThat(response.mappings().containsKey("index"), equalTo(true));
        assertThat(response.mappings().get("index").size(), equalTo(0));
    }

    static XContentBuilder getMappingForType(String type) throws IOException {
        return jsonBuilder().startObject().startObject(type).startObject("properties")
                .startObject("field1").field("type", "text").endObject()
                .endObject().endObject().endObject();
    }

    public void testGetMappingsWithBlocks() throws IOException {
        client().admin().indices().prepareCreate("test")
                .addMapping("_doc", getMappingForType("_doc"))
                .execute().actionGet();
        ensureGreen();

        for (String block : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE, SETTING_READ_ONLY)) {
            try {
                enableIndexBlock("test", block);
                GetMappingsResponse response = client().admin().indices().prepareGetMappings().execute().actionGet();
                assertThat(response.mappings().size(), equalTo(1));
                assertThat(response.mappings().get("test").size(), equalTo(1));
            } finally {
                disableIndexBlock("test", block);
            }
        }

        try {
            enableIndexBlock("test", SETTING_BLOCKS_METADATA);
            assertBlocked(client().admin().indices().prepareGetMappings(), INDEX_METADATA_BLOCK);
        } finally {
            disableIndexBlock("test", SETTING_BLOCKS_METADATA);
        }
    }
}
