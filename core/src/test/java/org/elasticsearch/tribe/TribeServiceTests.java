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

package org.elasticsearch.tribe;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestCustomMetaData;

import java.util.EnumSet;

public class TribeServiceTests extends ESTestCase {
    public void testMinimalSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, Settings.EMPTY);
        assertEquals("some/path", clientSettings.get("path.home"));
        assertEquals("nodename/tribe1", clientSettings.get("node.name"));
        assertEquals("tribe1", clientSettings.get("tribe.name"));
        assertFalse(NetworkModule.HTTP_ENABLED.get(clientSettings));
        assertEquals("false", clientSettings.get("node.master"));
        assertEquals("false", clientSettings.get("node.data"));
        assertEquals("false", clientSettings.get("node.ingest"));
        assertEquals("false", clientSettings.get("node.local_storage"));
        assertEquals("3707202549613653169", clientSettings.get("node.id.seed")); // should be fixed by the parent id and tribe name
        assertEquals(9, clientSettings.getAsMap().size());
    }

    public void testEnvironmentSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path")
            .put("path.conf", "conf/path")
            .put("path.scripts", "scripts/path")
            .put("path.logs", "logs/path").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, Settings.EMPTY);
        assertEquals("some/path", clientSettings.get("path.home"));
        assertEquals("conf/path", clientSettings.get("path.conf"));
        assertEquals("scripts/path", clientSettings.get("path.scripts"));
        assertEquals("logs/path", clientSettings.get("path.logs"));

        Settings tribeSettings = Settings.builder()
            .put("path.home", "alternate/path").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, tribeSettings);
        });
        assertTrue(e.getMessage(), e.getMessage().contains("Setting [path.home] not allowed in tribe client"));
    }

    public void testPassthroughSettings() {
        Settings globalSettings = Settings.builder()
            .put("node.name", "nodename")
            .put("path.home", "some/path")
            .put("network.host", "0.0.0.0")
            .put("network.bind_host", "1.1.1.1")
            .put("network.publish_host", "2.2.2.2")
            .put("transport.host", "3.3.3.3")
            .put("transport.bind_host", "4.4.4.4")
            .put("transport.publish_host", "5.5.5.5").build();
        Settings clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, Settings.EMPTY);
        assertEquals("0.0.0.0", clientSettings.get("network.host"));
        assertEquals("1.1.1.1", clientSettings.get("network.bind_host"));
        assertEquals("2.2.2.2", clientSettings.get("network.publish_host"));
        assertEquals("3.3.3.3", clientSettings.get("transport.host"));
        assertEquals("4.4.4.4", clientSettings.get("transport.bind_host"));
        assertEquals("5.5.5.5", clientSettings.get("transport.publish_host"));

        // per tribe client overrides still work
        Settings tribeSettings = Settings.builder()
            .put("network.host", "3.3.3.3")
            .put("network.bind_host", "4.4.4.4")
            .put("network.publish_host", "5.5.5.5")
            .put("transport.host", "6.6.6.6")
            .put("transport.bind_host", "7.7.7.7")
            .put("transport.publish_host", "8.8.8.8").build();
        clientSettings = TribeService.buildClientSettings("tribe1", "parent_id", globalSettings, tribeSettings);
        assertEquals("3.3.3.3", clientSettings.get("network.host"));
        assertEquals("4.4.4.4", clientSettings.get("network.bind_host"));
        assertEquals("5.5.5.5", clientSettings.get("network.publish_host"));
        assertEquals("6.6.6.6", clientSettings.get("transport.host"));
        assertEquals("7.7.7.7", clientSettings.get("transport.bind_host"));
        assertEquals("8.8.8.8", clientSettings.get("transport.publish_host"));
    }

    public void testReduceCustomMetaData() {
        MetaData existingMetaData = randomMetaData(new CustomMetaData1("data1"));
        MetaData newMetaData = randomMetaData(new CustomMetaData1("data2"));
        ImmutableOpenMap<String, MetaData.Custom> reducedCustoms =
                TribeService.reduceCustomMetaData(existingMetaData.customs(), newMetaData.customs(),
                        (existingCustoms, newCustoms) -> {
                            existingCustoms.put(CustomMetaData1.TYPE, newCustoms.get(CustomMetaData1.TYPE));
                            return existingCustoms;
                        }
                );
        assertTrue(reducedCustoms != existingMetaData.customs());
        assertEquals(((TestCustomMetaData) reducedCustoms.get(CustomMetaData1.TYPE)).getData(), "data2");
    }

    public void testNoopReduceCustomMetaData() {
        MetaData existingMetaData = randomMetaData(new CustomMetaData1("data1"));
        MetaData newMetaData = randomMetaData(new CustomMetaData1("data2"));
        ImmutableOpenMap<String, MetaData.Custom> reducedCustoms =
                TribeService.reduceCustomMetaData(existingMetaData.customs(), newMetaData.customs(),
                        (existingCustoms, newCustoms) -> existingCustoms
                );
        assertTrue(reducedCustoms == existingMetaData.customs());
        assertEquals(((TestCustomMetaData) reducedCustoms.get(CustomMetaData1.TYPE)).getData(), "data1");
    }

    public void testReduceMultipleCustomMetaData() {
        MetaData existingMetaData = randomMetaData(new CustomMetaData1("existing_data1"), new CustomMetaData2("existing_data2"));
        MetaData newMetaData = randomMetaData(new CustomMetaData1("new_data1"), new CustomMetaData2("new_data2"));
        ImmutableOpenMap<String, MetaData.Custom> reducedCustoms =
                TribeService.reduceCustomMetaData(existingMetaData.customs(), newMetaData.customs(),
                        (existingCustoms, newCustoms) -> {
                            existingCustoms.put(CustomMetaData1.TYPE, newCustoms.get(CustomMetaData1.TYPE));
                            existingCustoms.put(CustomMetaData2.TYPE, newCustoms.get(CustomMetaData2.TYPE));
                            return existingCustoms;
                        }
                );
        assertTrue(reducedCustoms != existingMetaData.customs());
        assertEquals(((TestCustomMetaData) reducedCustoms.get(CustomMetaData1.TYPE)).getData(), "new_data1");
        assertEquals(((TestCustomMetaData) reducedCustoms.get(CustomMetaData2.TYPE)).getData(), "new_data2");
    }

    private static class CustomMetaData1 extends TestCustomMetaData {
        public static final String TYPE = "custom_md_1";

        protected CustomMetaData1(String data) {
            super(data);
        }

        @Override
        protected TestCustomMetaData newTestCustomMetaData(String data) {
            return new CustomMetaData1(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static class CustomMetaData2 extends TestCustomMetaData {
        public static final String TYPE = "custom_md_2";

        protected CustomMetaData2(String data) {
            super(data);
        }

        @Override
        protected TestCustomMetaData newTestCustomMetaData(String data) {
            return new CustomMetaData2(data);
        }

        @Override
        public String type() {
            return TYPE;
        }

        @Override
        public EnumSet<MetaData.XContentContext> context() {
            return EnumSet.of(MetaData.XContentContext.GATEWAY);
        }
    }

    private static MetaData randomMetaData(TestCustomMetaData... customMetaDatas) {
        MetaData.Builder builder = MetaData.builder();
        for (TestCustomMetaData customMetaData : customMetaDatas) {
            builder.putCustom(customMetaData.type(), customMetaData);
        }
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            builder.put(IndexMetaData.builder(randomAsciiOfLength(10))
                            .settings(settings(Version.CURRENT))
                            .numberOfReplicas(randomIntBetween(0, 3))
                            .numberOfShards(randomIntBetween(1, 5))
            );
        }
        return builder.build();
    }
}
