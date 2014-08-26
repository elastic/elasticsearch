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

package org.elasticsearch.index.mapper;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class FileBasedMappingsTests extends ElasticsearchTestCase {

    private static final String NAME = FileBasedMappingsTests.class.getSimpleName();

    public void testFileBasedMappings() throws Exception {
        File configDir = Files.createTempDir();
        File mappingsDir = new File(configDir, "mappings");
        File indexMappings = new File(new File(mappingsDir, "index"), "type.json");
        File defaultMappings = new File(new File(mappingsDir, "_default"), "type.json");
        try {
            indexMappings.getParentFile().mkdirs();
            defaultMappings.getParentFile().mkdirs();

            try (XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, new FileOutputStream(indexMappings))) {
                builder.startObject()
                        .startObject("type")
                            .startObject("properties")
                                .startObject("f")
                                    .field("type", "string")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject();
            }

            try (XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, new FileOutputStream(defaultMappings))) {
                builder.startObject()
                        .startObject("type")
                            .startObject("properties")
                                .startObject("g")
                                    .field("type", "string")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject();
            }

            Settings settings = ImmutableSettings.builder()
                    .put(ClusterName.SETTING, NAME)
                    .put("node.name", NAME)
                    .put("path.conf", configDir.getAbsolutePath())
                    .put("http.enabled", false)
                    .put("index.store.type", "ram")
                    .put("gateway.type", "none")
                    .build();

            try (Node node = NodeBuilder.nodeBuilder().local(true).data(true).settings(settings).build()) {
                node.start();

                assertAcked(node.client().admin().indices().prepareCreate("index").addMapping("type", "h", "type=string").get());
                final GetMappingsResponse response = node.client().admin().indices().prepareGetMappings("index").get();
                assertTrue(response.mappings().toString(), response.mappings().containsKey("index"));
                MappingMetaData mappings = response.mappings().get("index").get("type");
                assertNotNull(mappings);
                Map<?, ?> properties = (Map<?, ?>) (mappings.getSourceAsMap().get("properties"));
                assertNotNull(properties);
                assertEquals(ImmutableSet.of("f", "g", "h"), properties.keySet());
            }
        } finally {
            FileSystemUtils.deleteRecursively(configDir);
        }
    }

}
