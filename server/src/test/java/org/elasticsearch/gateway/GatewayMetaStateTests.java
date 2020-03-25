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

package org.elasticsearch.gateway;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.MetaDataUpgrader;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetaData;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;

public class GatewayMetaStateTests extends ESTestCase {

    public void testUpdateTemplateMetaDataOnUpgrade() {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.singletonList(
                        templates -> {
                            templates.put("added_test_template", IndexTemplateMetaData.builder("added_test_template")
                                .patterns(randomIndexPatterns()).build());
                            return templates;
                        }
                    ));

        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertNotSame(upgrade, metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertTrue(upgrade.templates().containsKey("added_test_template"));
    }

    public void testNoMetaDataUpgrade() {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertSame(upgrade, metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testCustomMetaDataValidation() {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.emptyList());
        try {
            GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("custom meta data too old"));
        }
    }

    public void testIndexMetaDataUpgrade() {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.emptyList());
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(true), metaDataUpgrader);
        assertNotSame(upgrade, metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertFalse(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testCustomMetaDataNoChange() {
        MetaData metaData = randomMetaData(new CustomMetaData1("data"));
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.singletonList(HashMap::new));
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertSame(upgrade, metaData);
        assertTrue(MetaData.isGlobalStateEquals(upgrade, metaData));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    public void testIndexTemplateValidation() {
        MetaData metaData = randomMetaData();
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Collections.singletonList(
                        customs -> {
                            throw new IllegalStateException("template is incompatible");
                        }));
        String message = expectThrows(IllegalStateException.class,
            () -> GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader)).getMessage();
        assertThat(message, equalTo("template is incompatible"));
    }


    public void testMultipleIndexTemplateUpgrade() {
        final MetaData metaData;
        switch (randomIntBetween(0, 2)) {
            case 0:
                metaData = randomMetaDataWithIndexTemplates("template1", "template2");
                break;
            case 1:
                metaData = randomMetaDataWithIndexTemplates(randomBoolean() ? "template1" : "template2");
                break;
            case 2:
                metaData = randomMetaData();
                break;
            default:
                throw new IllegalStateException("should never happen");
        }
        MetaDataUpgrader metaDataUpgrader = new MetaDataUpgrader(Arrays.asList(
                indexTemplateMetaDatas -> {
                    indexTemplateMetaDatas.put("template1", IndexTemplateMetaData.builder("template1")
                        .patterns(randomIndexPatterns())
                        .settings(Settings.builder().put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 20).build())
                        .build());
                    return indexTemplateMetaDatas;

                },
                indexTemplateMetaDatas -> {
                    indexTemplateMetaDatas.put("template2", IndexTemplateMetaData.builder("template2")
                        .patterns(randomIndexPatterns())
                        .settings(Settings.builder().put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 10).build()).build());
                    return indexTemplateMetaDatas;

                }
            ));
        MetaData upgrade = GatewayMetaState.upgradeMetaData(metaData, new MockMetaDataIndexUpgradeService(false), metaDataUpgrader);
        assertNotSame(upgrade, metaData);
        assertFalse(MetaData.isGlobalStateEquals(upgrade, metaData));
        assertNotNull(upgrade.templates().get("template1"));
        assertThat(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.get(upgrade.templates().get("template1").settings()), equalTo(20));
        assertNotNull(upgrade.templates().get("template2"));
        assertThat(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.get(upgrade.templates().get("template2").settings()), equalTo(10));
        for (IndexMetaData indexMetaData : upgrade) {
            assertTrue(metaData.hasIndexMetaData(indexMetaData));
        }
    }

    private static class MockMetaDataIndexUpgradeService extends MetaDataIndexUpgradeService {
        private final boolean upgrade;

        MockMetaDataIndexUpgradeService(boolean upgrade) {
            super(Settings.EMPTY, null, null, null);
            this.upgrade = upgrade;
        }

        @Override
        public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData, Version minimumIndexCompatibilityVersion) {
            return upgrade ? IndexMetaData.builder(indexMetaData).build() : indexMetaData;
        }
    }

    private static class CustomMetaData1 extends TestCustomMetaData {
        public static final String TYPE = "custom_md_1";

        CustomMetaData1(String data) {
            super(data);
        }

        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static MetaData randomMetaData(TestCustomMetaData... customMetaDatas) {
        MetaData.Builder builder = MetaData.builder();
        for (TestCustomMetaData customMetaData : customMetaDatas) {
            builder.putCustom(customMetaData.getWriteableName(), customMetaData);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetaData.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static MetaData randomMetaDataWithIndexTemplates(String... templates) {
        MetaData.Builder builder = MetaData.builder();
        for (String template : templates) {
            IndexTemplateMetaData templateMetaData = IndexTemplateMetaData.builder(template)
                .settings(settings(Version.CURRENT)
                    .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 3))
                    .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), randomIntBetween(1, 5)))
                .patterns(randomIndexPatterns())
                .build();
            builder.put(templateMetaData);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(
                IndexMetaData.builder(randomAlphaOfLength(10))
                    .settings(settings(Version.CURRENT))
                    .numberOfReplicas(randomIntBetween(0, 3))
                    .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }

    private static List<String> randomIndexPatterns() {
        return Arrays.asList(Objects.requireNonNull(generateRandomStringArray(10, 100, false, false)));
    }
}
