/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
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

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.elasticsearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DiscoveryNodeFiltersTests extends ESTestCase {

    private static TransportAddress localAddress;

    @BeforeClass
    public static void createLocalAddress() throws UnknownHostException {
        localAddress = new TransportAddress(InetAddress.getByName("192.1.1.54"), 9999);
    }

    @AfterClass
    public static void releaseLocalAddress() {
        localAddress = null;
    }

    public void testNameMatch() {
        Settings settings = Settings.builder().put("xxx.name", "name1").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("id1").name("name1").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(true));

        node = DiscoveryNodeUtils.builder("id2").name("name2").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIdMatch() {
        Settings settings = Settings.builder().put("xxx._id", "id1").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("id1").name("name1").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(true));

        node = DiscoveryNodeUtils.builder("id2").name("name2").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIdOrNameMatch() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._id", "id1,blah").put("xxx.name", "blah,name2").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("id1").name("name1").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(true));

        node = DiscoveryNodeUtils.builder("id2").name("name2").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(true));

        node = DiscoveryNodeUtils.builder("id3").name("name3").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testTagAndGroupMatch() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx.group", "B").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "B");
        DiscoveryNode node = DiscoveryNodeUtils.builder("id1").name("name1").attributes(attributes).roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(true));

        attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "B");
        attributes.put("name", "X");
        node = DiscoveryNodeUtils.builder("id2").name("name2").attributes(attributes).roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(true));

        attributes = new HashMap<>();
        attributes.put("tag", "A");
        attributes.put("group", "F");
        attributes.put("name", "X");
        node = DiscoveryNodeUtils.builder("id3").name("name3").attributes(attributes).roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(false));

        node = DiscoveryNodeUtils.builder("id4").name("name4").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testStarMatch() {
        Settings settings = Settings.builder().put("xxx.name", "*").build();
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("id1").name("name1").roles(emptySet()).build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringMatchingAnd() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "A").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringNotMatching() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "B").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpBindFilteringNotMatchingAnd() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "A").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "8.8.8.8").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpBindFilteringMatchingOr() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.54").put("xxx.tag", "A").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpBindFilteringNotMatchingOr() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx.tag", "A").put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "8.8.8.8").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx._publish_ip", "192.1.1.54").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringNotMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx._publish_ip", "8.8.8.8").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testIpPublishFilteringMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._publish_ip", "192.1.1.54").put("xxx.tag", "A").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testHostNameFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._host", "A").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("A", "192.1.1.54", localAddress)
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testHostAddressFilteringMatchingAnd() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._host", "192.1.1.54").build());
        DiscoveryNodeFilters filters = buildFromSettings(AND, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("A", "192.1.1.54", localAddress)
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringNotMatchingOr() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx.tag", "A").put("xxx._publish_ip", "8.8.8.8").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "A"))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testIpPublishFilteringMatchingWildcard() {
        boolean matches = randomBoolean();
        Settings settings = shuffleSettings(Settings.builder().put("xxx._publish_ip", matches ? "192.1.*" : "192.2.*").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(matches));
    }

    public void testCommaSeparatedValuesTrimmed() {
        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "B"))
            .roles(emptySet())
            .build();

        Settings settings = shuffleSettings(
            Settings.builder()
                .put("xxx." + randomFrom("_ip", "_host_ip", "_publish_ip"), "192.1.1.1, 192.1.1.54")
                .put("xxx.tag", "A, B")
                .build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);
        assertTrue(filters.match(node));
    }

    public void testOnlyAttributeValueFilter() {
        List<String> keys = randomSubsetOf(DiscoveryNodeFilters.NON_ATTRIBUTE_NAMES);
        if (keys.isEmpty() || randomBoolean()) {
            keys.add("tag");
        }
        Settings.Builder builder = Settings.builder();
        keys.forEach(key -> builder.put("xxx." + key, "1.2.3.4"));
        DiscoveryNodeFilters discoveryNodeFilters = buildFromSettings(DiscoveryNodeFilters.OpType.AND, "xxx.", builder.build());
        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.1.1.54", localAddress)
            .attributes(singletonMap("tag", "1.2.3.4"))
            .roles(emptySet())
            .build();

        assertThat(discoveryNodeFilters.isOnlyAttributeValueFilter(), is(discoveryNodeFilters.match(node)));
    }

    public void testNormalizesIPAddressFilters() {
        Settings settings = shuffleSettings(
            Settings.builder().put("xxx." + randomFrom("_ip", "_host_ip"), "fdbd:dc00:111:222:0:0:0:333").build()
        );
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "fdbd:dc00:111:222::333", localAddress)
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testNormalizesIPAddressFiltersForPublishIp() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._publish_ip", "fdbd:dc00:111:222:0:0:0:333").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("", "192.168.0.1", new TransportAddress(InetAddresses.forString("fdbd:dc00:111:222::333"), 9300))
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    public void testHostnameWhichLooksLikeIpv6DoesNotGetMatched() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._name", "fdbd:dc00:111:222:0:0:0:333").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("fdbd:dc00:111:222::333", "192.168.0.1", localAddress)
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(false));
    }

    public void testHostnameGetMatchedAndNotAffectedByNormalizing() {
        Settings settings = shuffleSettings(Settings.builder().put("xxx._host", "test-host").build());
        DiscoveryNodeFilters filters = buildFromSettings(OR, "xxx.", settings);

        DiscoveryNode node = DiscoveryNodeUtils.builder("")
            .name("")
            .ephemeralId("")
            .address("test-host", "192.168.0.1", localAddress)
            .roles(emptySet())
            .build();
        assertThat(filters.match(node), equalTo(true));
    }

    private Settings shuffleSettings(Settings source) {
        Settings.Builder settings = Settings.builder();
        List<String> keys = new ArrayList<>(source.keySet());
        Collections.shuffle(keys, random());
        for (String o : keys) {
            settings.put(o, source.get(o));
        }
        return settings.build();
    }

    public static DiscoveryNodeFilters buildFromSettings(DiscoveryNodeFilters.OpType opType, String prefix, Settings settings) {
        var values = Setting.prefixKeySetting(prefix, key -> Setting.stringListSetting(key)).getAsMap(settings);
        return DiscoveryNodeFilters.buildFromKeyValues(opType, values);
    }
}
