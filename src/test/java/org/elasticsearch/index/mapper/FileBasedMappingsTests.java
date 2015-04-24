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

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class FileBasedMappingsTests extends ElasticsearchTestCase {

    private static final String NAME = FileBasedMappingsTests.class.getSimpleName();

    public void testFileBasedMappings() throws Exception {
        Path configDir = createTempDir();
        Path mappingsDir = configDir.resolve("mappings");
        Path indexMappings = mappingsDir.resolve("index").resolve("type.json");
        Path defaultMappings = mappingsDir.resolve("_default").resolve("type.json");
        try {
            Files.createDirectories(indexMappings.getParent());
            Files.createDirectories(defaultMappings.getParent());

            try (OutputStream stream = Files.newOutputStream(indexMappings);
                 XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, stream)) {
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

            try (OutputStream stream = Files.newOutputStream(defaultMappings);
                 XContentBuilder builder = new XContentBuilder(JsonXContent.jsonXContent, stream)) {
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
                    .put("path.home", createTempDir())
                    .put("path.conf", configDir.toAbsolutePath())
                    .put("http.enabled", false)
                    .build();

            try (Node node = NodeBuilder.nodeBuilder().local(true).data(true).settings(settings).node()) {

                assertAcked(node.client().admin().indices().prepareCreate("index").addMapping("type", "h", "type=string").get());
                try {
                    final GetMappingsResponse response = node.client().admin().indices().prepareGetMappings("index").get();
                    assertTrue(response.mappings().toString(), response.mappings().containsKey("index"));
                    MappingMetaData mappings = response.mappings().get("index").get("type");
                    assertNotNull(mappings);
                    Map<?, ?> properties = (Map<?, ?>) (mappings.getSourceAsMap().get("properties"));
                    assertNotNull(properties);
                    assertEquals(ImmutableSet.of("f", "g", "h"), properties.keySet());
                } finally {
                    // remove the index...
                    assertAcked(node.client().admin().indices().prepareDelete("index"));
                }
            }
        } finally {
            IOUtils.rm(configDir);
        }
    }

}
