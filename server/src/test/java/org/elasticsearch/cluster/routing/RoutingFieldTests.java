/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Reading the routing value of {@link IndexSettings#ROUTING_FIELD} from a document, and routing by it.
 */
public class RoutingFieldTests extends ESTestCase {

    public void testKeyword() {
        RoutingField field = new RoutingField("session_id", RoutingField.Type.KEYWORD);
        assertThat(extract(field, """
            {"session_id": "s-100", "other": 1}"""), equalTo("s-100"));
        // a number in a keyword field is indexed as written
        assertThat(extract(field, """
            {"session_id": 1e3}"""), equalTo("1e3"));
        assertThat(extract(field, """
            {"session_id": ["s-100"]}"""), equalTo("s-100"));
    }

    public void testNestedAndDottedPaths() {
        RoutingField field = new RoutingField("session.id", RoutingField.Type.KEYWORD);
        assertThat(extract(field, """
            {"session": {"id": "s-1", "user": "u"}}"""), equalTo("s-1"));
        assertThat(extract(field, """
            {"session.id": "s-2"}"""), equalTo("s-2"));
    }

    public void testIntegral() {
        RoutingField field = new RoutingField("n", RoutingField.Type.INTEGRAL);
        assertThat(extract(field, """
            {"n": 42}"""), equalTo("42"));
        assertThat(extract(field, """
            {"n": "42"}"""), equalTo("42"));
        assertThat(extract(field, """
            {"n": 42.0}"""), equalTo("42"));
        assertThat(extract(field, """
            {"n": -9223372036854775808}"""), equalTo("-9223372036854775808"));
        assertThat(expectError(field, """
            {"n": 42.5}"""), containsString("routing field [n] must be an integer, found [42.5]"));
        assertThat(expectError(field, """
            {"n": "forty-two"}"""), containsString("must be an integer, found [forty-two]"));
        assertThat(expectError(field, """
            {"n": true}"""), containsString("must be an integer, found [true]"));
        assertThat(expectError(field, """
            {"n": 9223372036854775808}"""), containsString("must be an integer"));
    }

    public void testIp() {
        RoutingField field = new RoutingField("ip", RoutingField.Type.IP);
        assertThat(extract(field, """
            {"ip": "10.0.0.1"}"""), equalTo("10.0.0.1"));
        assertThat(extract(field, """
            {"ip": "::ffff:10.0.0.1"}"""), equalTo("10.0.0.1"));
        assertThat(extract(field, """
            {"ip": "2001:0db8:0000:0000:0000:0000:0000:0001"}"""), equalTo("2001:db8::1"));
        assertThat(expectError(field, """
            {"ip": "not-an-ip"}"""), containsString("routing field [ip] must be an IP address, found [not-an-ip]"));
        assertThat(expectError(field, """
            {"ip": 10}"""), containsString("must be an IP address"));
    }

    public void testMissingOrMultiValued() {
        RoutingField field = new RoutingField("session_id", RoutingField.Type.KEYWORD);
        assertThat(expectError(field, """
            {"other": "x"}"""), containsString("routing field [session_id] is missing or null"));
        assertThat(expectError(field, """
            {"session_id": null}"""), containsString("routing field [session_id] is missing or null"));
        assertThat(expectError(field, """
            {"session_id": ["a", "b"]}"""), containsString("routing field [session_id] must have a single value, found [2]"));
    }

    public void testTypeFromMapping() {
        IndexMetadata metadata = index("""
            {"_doc": {"properties": {
              "session_id": {"type": "keyword"},
              "n": {"type": "integer"},
              "client": {"properties": {"ip": {"type": "ip"}}},
              "title": {"type": "text"}
            }}}""");
        assertThat(RoutingField.typeFromMapping(metadata.mapping(), "session_id"), equalTo(RoutingField.Type.KEYWORD));
        assertThat(RoutingField.typeFromMapping(metadata.mapping(), "n"), equalTo(RoutingField.Type.INTEGRAL));
        assertThat(RoutingField.typeFromMapping(metadata.mapping(), "client.ip"), equalTo(RoutingField.Type.IP));
        assertThat(RoutingField.typeFromMapping(metadata.mapping(), "title"), nullValue());
        assertThat(RoutingField.typeFromMapping(metadata.mapping(), "missing"), nullValue());
        assertThat(RoutingField.typeFromMapping(null, "session_id"), nullValue());
    }

    /**
     * The coordinating node fills {@code _routing} from the field, and the shard is the one {@code ?routing=<value>} picks.
     */
    public void testRoutingFilledFromField() {
        IndexMetadata metadata = index(KEYWORD_MAPPING);
        IndexRouting routing = IndexRouting.fromIndexMetadata(metadata);
        IndexRequest request = request("""
            {"session_id": "s-100"}""", null);
        request.preRoutingProcess(routing);
        assertThat(request.routing(), equalTo("s-100"));
        int shard = routing.indexShard(request);
        assertThat(shard, equalTo(routing.getShard("any-id", "s-100")));
    }

    public void testMatchingRoutingAccepted() {
        IndexRouting routing = IndexRouting.fromIndexMetadata(index(KEYWORD_MAPPING));
        IndexRequest request = request("""
            {"session_id": "s-100"}""", "s-100");
        request.preRoutingProcess(routing);
        assertThat(request.routing(), equalTo("s-100"));
    }

    public void testMismatchedRoutingRejected() {
        IndexRouting routing = IndexRouting.fromIndexMetadata(index(KEYWORD_MAPPING));
        IndexRequest request = request("""
            {"session_id": "s-100"}""", "s-200");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> request.preRoutingProcess(routing));
        assertThat(e.getMessage(), containsString("routing [s-200] does not match the value [s-100] of routing field [session_id]"));
    }

    public void testUnmappedRoutingFieldRejectsWrites() {
        IndexRouting routing = IndexRouting.fromIndexMetadata(index("""
            {"_doc": {"properties": {"other": {"type": "keyword"}}}}"""));
        IndexRequest request = request("""
            {"session_id": "s-100"}""", null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> request.preRoutingProcess(routing));
        assertThat(e.getMessage(), containsString("routing field [session_id] of index [test] must be mapped as one of"));
    }

    /**
     * Equal values route to equal shards across indices, whatever the numeric width or spelling.
     */
    public void testIntegralValuesRouteAlike() {
        IndexRouting longs = IndexRouting.fromIndexMetadata(index("""
            {"_doc": {"_routing": {"required": true}, "properties": {"session_id": {"type": "long"}}}}"""));
        IndexRouting ints = IndexRouting.fromIndexMetadata(index("""
            {"_doc": {"_routing": {"required": true}, "properties": {"session_id": {"type": "integer"}}}}"""));
        for (int i = 0; i < 20; i++) {
            int value = randomInt();
            IndexRequest asNumber = request("{\"session_id\": " + value + "}", null);
            IndexRequest asString = request("{\"session_id\": \"" + value + "\"}", null);
            asNumber.preRoutingProcess(longs);
            asString.preRoutingProcess(ints);
            assertThat(asNumber.routing(), equalTo(Integer.toString(value)));
            assertThat(longs.indexShard(asNumber), equalTo(ints.indexShard(asString)));
        }
    }

    private static final String KEYWORD_MAPPING = """
        {"_doc": {"_routing": {"required": true}, "properties": {"session_id": {"type": "keyword"}}}}""";

    private static IndexMetadata index(String mapping) {
        return IndexMetadata.builder("test")
            .settings(indexSettings(IndexVersion.current(), 3, 0).put(IndexSettings.ROUTING_FIELD.getKey(), "session_id"))
            .putMapping(mapping)
            .build();
    }

    private static IndexRequest request(String source, String routing) {
        return new IndexRequest("test").source(new BytesArray(source), XContentType.JSON).routing(routing);
    }

    private static String extract(RoutingField field, String source) {
        return field.extract(XContentType.JSON, new BytesArray(source));
    }

    private static String expectError(RoutingField field, String source) {
        return expectThrows(IllegalArgumentException.class, () -> extract(field, source)).getMessage();
    }
}
