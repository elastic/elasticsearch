/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link IndexSettings#ROUTING_FIELD} through the real write and read APIs: index, bulk, update and get.
 */
public class RoutingFieldSingleNodeTests extends ESSingleNodeTestCase {

    private static final int SHARDS = 3;

    private void createRoutedIndex(String index, String type) {
        createIndex(
            index,
            indicesAdmin().prepareCreate(index)
                .setSettings(Settings.builder().put("index.number_of_shards", SHARDS).put(IndexSettings.ROUTING_FIELD.getKey(), "key"))
                .setMapping(String.format(Locale.ROOT, """
                    {"_routing": {"required": true}, "properties": {"key": {"type": "%s"}, "n": {"type": "long"}}}""", type))
        );
    }

    /**
     * No routing on the request: the value of the field becomes the routing, the document lands on the shard that
     * {@code ?routing=<value>} names, and it is read back with that routing.
     */
    public void testRoutingFilledFromField() {
        createRoutedIndex("sessions", "keyword");
        var response = client().prepareIndex("sessions").setId("1").setSource("key", "s-100", "n", 1).get();
        assertThat(response.getShardId().id(), equalTo(expectedShard("sessions", "s-100")));
        GetResponse get = client().prepareGet("sessions", "1").setRouting("s-100").get();
        assertTrue(get.isExists());
        assertThat(get.getFields().get("_routing").getValue(), equalTo("s-100"));
    }

    public void testBulkRejectsMismatchedRouting() {
        createRoutedIndex("sessions", "keyword");
        BulkResponse bulk = client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(client().prepareIndex("sessions").setId("1").setSource("key", "s-100"))
            .add(client().prepareIndex("sessions").setId("2").setSource("key", "s-200").setRouting("s-200"))
            .add(client().prepareIndex("sessions").setId("3").setSource("key", "s-300").setRouting("s-999"))
            .add(client().prepareIndex("sessions").setId("4").setSource("other", "x"))
            .get();
        assertFalse(bulk.getItems()[0].isFailed());
        assertFalse(bulk.getItems()[1].isFailed());
        assertThat(
            bulk.getItems()[2].getFailureMessage(),
            containsString("routing [s-999] does not match the value [s-300] of routing field [key]")
        );
        assertThat(bulk.getItems()[3].getFailureMessage(), containsString("routing field [key] is missing or null"));
    }

    /**
     * Updates rebuild the document on the primary with the stored routing, without the coordinator's routing step: the
     * data node rejects one that changes the routing field and accepts one that does not.
     */
    public void testUpdateCannotChangeRoutingField() {
        createRoutedIndex("sessions", "keyword");
        client().prepareIndex("sessions").setId("1").setSource("key", "s-100", "n", 1).get();

        client().prepareUpdate("sessions", "1").setRouting("s-100").setDoc(Map.of("n", 2)).get();
        assertThat(client().prepareGet("sessions", "1").setRouting("s-100").get().getSource().get("n"), equalTo(2));

        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareUpdate("sessions", "1").setRouting("s-100").setDoc(Map.of("key", "s-200")).get()
        );
        assertThat(e.getMessage(), containsString("routing [s-100] does not match the value [s-200] of routing field [key]"));
        assertThat(client().prepareGet("sessions", "1").setRouting("s-100").get().getSource().get("key"), equalTo("s-100"));
    }

    public void testIntegralRoutingField() {
        createRoutedIndex("events", "long");
        client().prepareIndex("events").setId("1").setSource("{\"key\": \"42\"}", XContentType.JSON).get();
        client().prepareIndex("events").setId("2").setSource("{\"key\": 42.0}", XContentType.JSON).get();
        assertTrue(client().prepareGet("events", "1").setRouting("42").get().isExists());
        assertTrue(client().prepareGet("events", "2").setRouting("42").get().isExists());
        Exception e = expectThrows(
            Exception.class,
            () -> client().prepareIndex("events").setId("3").setSource("{\"key\": 42.5}", XContentType.JSON).get()
        );
        assertThat(e.getMessage(), containsString("routing field [key] must be an integer, found [42.5]"));
    }

    public void testIpRoutingField() {
        createRoutedIndex("hosts", "ip");
        var response = client().prepareIndex("hosts").setId("1").setSource("key", "::ffff:10.0.0.1").get();
        assertThat(response.getShardId().id(), equalTo(expectedShard("hosts", "10.0.0.1")));
        assertTrue(client().prepareGet("hosts", "1").setRouting("10.0.0.1").get().isExists());
    }

    public void testIncompatibleSettingsRejected() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().prepareCreate("bad")
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", SHARDS)
                        .put(IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.getKey(), 2)
                        .put(IndexSettings.ROUTING_FIELD.getKey(), "key")
                )
                .setMapping("""
                    {"_routing": {"required": true}, "properties": {"key": {"type": "keyword"}}}""")
                .get()
        );
        assertThat(e.getMessage(), containsString("[index.routing_field] cannot be used with [index.routing_partition_size]"));
    }

    public void testUnsupportedMappingRejected() {
        Exception e = expectThrows(
            Exception.class,
            () -> indicesAdmin().prepareCreate("bad")
                .setSettings(Settings.builder().put(IndexSettings.ROUTING_FIELD.getKey(), "key"))
                .setMapping("""
                    {"_routing": {"required": true}, "properties": {"key": {"type": "double"}}}""")
                .get()
        );
        assertThat(e.getMessage(), containsString("routing field [key] of index [bad] must be mapped as one of"));
    }

    private int expectedShard(String index, String routing) {
        IndexMetadata metadata = getInstanceFromNode(ClusterService.class).state().metadata().getProject().index(index);
        return IndexRouting.fromIndexMetadata(metadata).getShard("ignored", routing);
    }
}
