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

package org.elasticsearch.cluster.node;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DiscoveryNodeFiltersTests extends ESTestCase {

    private static InetSocketTransportAddress localAddress;

    @BeforeClass
    public static void createLocalAddress() throws UnknownHostException {
        localAddress = new InetSocketTransportAddress(InetAddress.getByName("192.1.1.54"), 9999);
    }

    @AfterClass
    public static void releaseLocalAddress() {
        localAddress = null;
    }

    public void testNameMatch() {
        Settings settings = Settings.builder()
                .put("xxx.name", "name1")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIdMatch() {
        Settings settings = Settings.builder()
                .put("xxx._id", "id1")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIdOrNameMatch() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx._id", "id1,blah")
                .put("xxx.name", "blah,name2")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        node = new DiscoveryNode("name3", "id3", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testTagAndGroupMatch() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "A")
                .put("xxx.group", "B")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(AND, "xxx.", settings);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "B");
        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE,
                attributes, emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "B");
        attributes.put("name", "X");
        node = new DiscoveryNode("name2", "id2", DummyTransportAddress.INSTANCE,
                attributes, emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));

        attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "F");
        attributes.put("name", "X");
        node = new DiscoveryNode("name3", "id3", DummyTransportAddress.INSTANCE,
                attributes, emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));

        node = new DiscoveryNode("name4", "id4", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testStarMatch() {
        Settings settings = Settings.builder()
                .put("xxx.name", "*")
                .build();
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("name1", "id1", DummyTransportAddress.INSTANCE, emptyMap(), emptySet(), Version.CURRENT);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "A")
                .put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringNotMatching() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "B")
                .put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpBindFilteringNotMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "A")
                .put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "8.8.8.8")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpBindFilteringMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54")
                .put("xxx.tag", "A")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringNotMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "A")
                .put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "8.8.8.8")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "A")
                .put("xxx._publish_ip", "192.1.1.54")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringNotMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "A")
                .put("xxx._publish_ip", "8.8.8.8")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpPublishFilteringMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx._publish_ip", "192.1.1.54")
                .put("xxx.tag", "A")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringNotMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder()
                .put("xxx.tag", "A")
                .put("xxx._publish_ip", "8.8.8.8")
                .build());
        DiscoveryNodeFilters filters = DiscoveryNodeFilters.buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = new DiscoveryNode("", "", "", "192.1.1.54", localAddress, singletonMap("tag", "A"), emptySet(), null);
        assertThat(filters.match(node), equalTo(true));
    }

    private Settings shuffleSettings(Settings source) {
        Settings.Builder settings = Settings.builder();
        List<String> keys = new ArrayList<>(source.getAsMap().keySet());
        Collections.shuffle(keys, random());
        for (String o : keys) {
            settings.put(o, source.getAsMap().get(o));
        }
        return settings.build();
    }
}
