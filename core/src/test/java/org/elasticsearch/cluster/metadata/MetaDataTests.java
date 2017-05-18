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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MetaDataTests extends ESTestCase {

    public void testIndexAndAliasWithSameName() {
        IndexMetaData.Builder builder = IndexMetaData.builder("index")
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(AliasMetaData.builder("index").build());
        try {
            MetaData.builder().put(builder).build();
            fail("expection should have been thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("index and alias names need to be unique, but alias [index] and index [index] have the same name"));
        }
    }


    @Test
    public void testMetaDataTemplateSettingsUpgrade() throws Exception {
        MetaData metaData = MetaData.builder()
                .put(IndexTemplateMetaData.builder("t1").settings(
                        Settings.builder().put("index.translog.interval", 8000))).build();

        MetaData newMd = MetaData.addDefaultUnitsIfNeeded(Loggers.getLogger(MetaDataTests.class), metaData);

        assertThat(newMd.getTemplates().get("t1").getSettings().get("index.translog.interval"), is("8000ms"));
    }

    @Test
    public void testMetaDataTemplateMappingsUpgrade() throws Exception {
        Map<String, Object> mapping = new HashMap<String, Object>() {{
            put("obj_field", new HashMap<String, Object>(){{
                put("properties", new HashMap<String, Object>(){{
                    put("type", "object");
                    put("store", Boolean.TRUE);
                    put("doc_values", Boolean.FALSE);
                    put("index", "not_indexed");
                    put("dynamic", Boolean.TRUE);
                }});
            }});
            put("str_field", new HashMap<String, Object>(){{
                put("properties", new HashMap<String, Object>(){{
                    put("type", "string");
                    put("store", Boolean.TRUE);
                    put("doc_values", Boolean.TRUE);
                    put("index", "not_indexed");
                }});
            }});
        }};
        CompressedXContent xContent = new CompressedXContent(
            XContentFactory.jsonBuilder()
                .map(mapping)
                .bytes()
        );
        MetaData metaData = MetaData.builder()
            .put(IndexTemplateMetaData.builder("t2").putMapping("default", xContent))
            .build();

        MetaData newMd = MetaData.removeInvalidObjectPropertiesIfNeeded(Loggers.getLogger(MetaDataTests.class), metaData);
        byte[] newMappingBytes = newMd.getTemplates().get("t2").getMappings().get("default").uncompressed();
        Map<String, Object> newMapping = XContentFactory.xContent(newMappingBytes)
            .createParser(newMappingBytes)
            .map();

        // obj_field should have changed
        assertEquals(new HashMap<String, Object>(){{
            put("properties", new HashMap<String, Object>(){{
                put("type", "object");
                put("dynamic", Boolean.TRUE);
            }});
        }}, newMapping.get("obj_field"));
        // str_field must not have changed
        assertEquals(mapping.get("str_field"), newMapping.get("str_field"));

    }
}
