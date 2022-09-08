/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_DEST_IP;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_DEST_PORT;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_IANA_NUMBER;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_ICMP_CODE;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_ICMP_TYPE;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_SOURCE_IP;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_SOURCE_PORT;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_TARGET;
import static org.elasticsearch.ingest.common.CommunityIdProcessor.Factory.DEFAULT_TRANSPORT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CommunityIdProcessorTests extends ESTestCase {

    // NOTE: all test methods beginning with "testBeats" are intended to duplicate the unit tests for the Beats
    // community_id processor (see Github link below) to ensure that this processor produces the same values. To
    // the extent possible, these tests should be kept in sync.
    //
    // https://github.com/elastic/beats/blob/master/libbeat/processors/communityid/communityid_test.go

    private Map<String, Object> event;

    @Before
    public void setup() throws Exception {
        event = buildEvent();
    }

    private Map<String, Object> buildEvent() {
        event = new HashMap<>();
        var source = new HashMap<String, Object>();
        source.put("ip", "128.232.110.120");
        source.put("port", 34855);
        event.put("source", source);
        var destination = new HashMap<String, Object>();
        destination.put("ip", "66.35.250.204");
        destination.put("port", 80);
        event.put("destination", destination);
        var network = new HashMap<String, Object>();
        network.put("transport", "TCP");
        event.put("network", network);
        return event;
    }

    public void testBeatsValid() throws Exception {
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testBeatsSeed() throws Exception {
        testCommunityIdProcessor(event, 123, "1:hTSGlFQnR58UCk+NfKRZzA32dPg=");
    }

    public void testBeatsInvalidSourceIp() throws Exception {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("ip", 2162716280L);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("field [source.ip] of type [java.lang.Long] cannot be cast to [java.lang.String]"));
    }

    public void testBeatsInvalidSourcePort() throws Exception {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("port", 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid source port"));
    }

    public void testBeatsInvalidDestinationIp() throws Exception {
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        String invalidIp = "308.111.1.2.3";
        destination.put("ip", invalidIp);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("'" + invalidIp + "' is not an IP string literal"));
    }

    public void testBeatsInvalidDestinationPort() throws Exception {
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.put("port", null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        // slightly modified from the beats test in that this one reports the actual invalid value rather than '0'
        assertThat(e.getMessage(), containsString("invalid destination port [null]"));
    }

    public void testBeatsUnknownProtocol() throws Exception {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "xyz");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("could not convert string [xyz] to transport protocol"));
    }

    public void testBeatsIcmp() throws Exception {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "icmp");
        var icmp = new HashMap<String, Object>();
        icmp.put("type", 3);
        icmp.put("code", 3);
        event.put("icmp", icmp);
        testCommunityIdProcessor(event, "1:KF3iG9XD24nhlSy4r1TcYIr5mfE=");
    }

    public void testBeatsIcmpWithoutTypeOrCode() throws Exception {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "icmp");
        testCommunityIdProcessor(event, "1:PAE85ZfR4SbNXl5URZwWYyDehwU=");
    }

    public void testBeatsIgmp() throws Exception {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "igmp");
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.remove("port");
        testCommunityIdProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");
    }

    public void testBeatsProtocolNumberAsString() throws Exception {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.remove("port");
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "2");
        testCommunityIdProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");
    }

    public void testBeatsProtocolNumber() throws Exception {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.remove("port");
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", 2);
        testCommunityIdProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");
    }

    public void testBeatsIanaNumber() throws Exception {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.remove("transport");
        network.put("iana_number", CommunityIdProcessor.Transport.Tcp.getTransportNumber());
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testIpv6() throws Exception {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("ip", "2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.put("ip", "2001:0:9d38:6ab8:1c48:3a1c:a95a:b1c2");
        testCommunityIdProcessor(event, "1:YC1+javPJ2LpK5xVyw1udfT83Qs=");
    }

    public void testIcmpWithCodeEquivalent() throws Exception {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "icmp");
        var icmp = new HashMap<String, Object>();
        icmp.put("type", 10);
        icmp.put("code", 3);
        event.put("icmp", icmp);
        testCommunityIdProcessor(event, "1:L8wnzpmRHIESLqLBy+zTqW3Pmqs=");
    }

    public void testStringAndNumber() throws Exception {
        // iana
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.remove("transport");
        network.put("iana_number", CommunityIdProcessor.Transport.Tcp.getTransportNumber());
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        network.put("iana_number", Integer.toString(CommunityIdProcessor.Transport.Tcp.getTransportNumber()));
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        // protocol number
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.remove("port");
        @SuppressWarnings("unchecked")
        var network2 = (Map<String, Object>) event.get("network");
        network2.put("transport", 2);
        testCommunityIdProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");

        network2.put("transport", "2");
        testCommunityIdProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");

        // source port
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 34855);
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        source2.put("port", "34855");
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        // dest port
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var dest2 = (Map<String, Object>) event.get("destination");
        dest2.put("port", 80);
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        dest2.put("port", "80");
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        // icmp type and code
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var network3 = (Map<String, Object>) event.get("network");
        network3.put("transport", "icmp");
        var icmp = new HashMap<String, Object>();
        icmp.put("type", 3);
        icmp.put("code", 3);
        event.put("icmp", icmp);
        testCommunityIdProcessor(event, "1:KF3iG9XD24nhlSy4r1TcYIr5mfE=");

        icmp = new HashMap<String, Object>();
        icmp.put("type", "3");
        icmp.put("code", "3");
        event.put("icmp", icmp);
        testCommunityIdProcessor(event, "1:KF3iG9XD24nhlSy4r1TcYIr5mfE=");
    }

    public void testLongsForNumericValues() throws Exception {
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 34855L);
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testFloatsForNumericValues() throws Exception {
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 34855.0);
        testCommunityIdProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testInvalidPort() throws Exception {
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("port", 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid source port [0]"));

        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 65536);
        e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid source port [65536]"));

        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source3 = (Map<String, Object>) event.get("destination");
        source3.put("port", 0);
        e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid destination port [0]"));

        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source4 = (Map<String, Object>) event.get("destination");
        source4.put("port", 65536);
        e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid destination port [65536]"));
    }

    public void testIgnoreMissing() throws Exception {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.remove("transport");
        testCommunityIdProcessor(event, 0, null, true);
    }

    public void testIgnoreMissingIsFalse() throws Exception {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("ip");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testCommunityIdProcessor(event, 0, null, false));

        assertThat(e.getMessage(), containsString("field [ip] not present as part of path [source.ip]"));
    }

    private void testCommunityIdProcessor(Map<String, Object> source, String expectedHash) throws Exception {
        testCommunityIdProcessor(source, 0, expectedHash);
    }

    private void testCommunityIdProcessor(Map<String, Object> source, int seed, String expectedHash) throws Exception {
        testCommunityIdProcessor(source, seed, expectedHash, false);
    }

    private void testCommunityIdProcessor(Map<String, Object> source, int seed, String expectedHash, boolean ignoreMissing)
        throws Exception {

        var processor = new CommunityIdProcessor(
            null,
            null,
            DEFAULT_SOURCE_IP,
            DEFAULT_SOURCE_PORT,
            DEFAULT_DEST_IP,
            DEFAULT_DEST_PORT,
            DEFAULT_IANA_NUMBER,
            DEFAULT_TRANSPORT,
            DEFAULT_ICMP_TYPE,
            DEFAULT_ICMP_CODE,
            DEFAULT_TARGET,
            CommunityIdProcessor.toUint16(seed),
            ignoreMissing
        );

        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        IngestDocument output = processor.execute(input);

        String hash = output.getFieldValue(DEFAULT_TARGET, String.class, ignoreMissing);
        assertThat(hash, equalTo(expectedHash));
    }

    public void testTransportEnum() {
        for (CommunityIdProcessor.Transport t : CommunityIdProcessor.Transport.values()) {
            assertThat(CommunityIdProcessor.Transport.fromNumber(t.getTransportNumber()), equalTo(t));
        }
    }

    public void testIcmpTypeEnum() {
        for (CommunityIdProcessor.IcmpType i : CommunityIdProcessor.IcmpType.values()) {
            assertThat(CommunityIdProcessor.IcmpType.fromNumber(i.getType()), equalTo(i));
        }
    }
}
