/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.exception.ExceptionsHelper;
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
    // community_id processor (see GitHub link below) to ensure that this processor produces the same values. To
    // the extent possible, these tests should be kept in sync.
    //
    // https://github.com/elastic/beats/blob/master/libbeat/processors/communityid/communityid_test.go

    private Map<String, Object> event;

    @Before
    public void setup() {
        event = buildEvent();
    }

    private Map<String, Object> buildEvent() {
        var source = new HashMap<String, Object>();
        source.put("ip", "128.232.110.120");
        source.put("port", 34855);

        var destination = new HashMap<String, Object>();
        destination.put("ip", "66.35.250.204");
        destination.put("port", 80);

        var network = new HashMap<String, Object>();
        network.put("transport", "TCP");

        var event = new HashMap<String, Object>();
        event.put("source", source);
        event.put("destination", destination);
        event.put("network", network);
        return event;
    }

    public void testBeatsValid() {
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testBeatsSeed() {
        testProcessor(event, 123, "1:hTSGlFQnR58UCk+NfKRZzA32dPg=");
    }

    public void testBeatsInvalidSourceIp() {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("ip", 2162716280L);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("field [source.ip] of type [java.lang.Long] cannot be cast to [java.lang.String]"));
    }

    public void testBeatsInvalidSourcePort() {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("port", 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid source port"));
    }

    public void testBeatsInvalidDestinationIp() {
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        String invalidIp = "308.111.1.2.3";
        destination.put("ip", invalidIp);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("'" + invalidIp + "' is not an IP string literal"));
    }

    public void testBeatsInvalidDestinationPort() {
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.put("port", null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        // slightly modified from the beats test in that this one reports the actual invalid value rather than '0'
        assertThat(e.getMessage(), containsString("invalid destination port [null]"));
    }

    public void testBeatsUnknownProtocol() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "xyz");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("could not convert string [xyz] to transport protocol"));
    }

    public void testBeatsIcmp() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "icmp");
        var icmp = new HashMap<String, Object>();
        icmp.put("type", 3);
        icmp.put("code", 3);
        event.put("icmp", icmp);
        testProcessor(event, "1:KF3iG9XD24nhlSy4r1TcYIr5mfE=");
    }

    public void testBeatsIcmpWithoutTypeOrCode() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "icmp");
        testProcessor(event, "1:PAE85ZfR4SbNXl5URZwWYyDehwU=");
    }

    public void testBeatsIgmp() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "igmp");
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.remove("port");
        testProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");
    }

    public void testBeatsProtocolNumberAsString() {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.remove("port");
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "2");
        testProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");
    }

    public void testBeatsProtocolNumber() {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.remove("port");
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", 2);
        testProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");
    }

    public void testBeatsIanaNumberProtocolTCP() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.remove("transport");
        network.put("iana_number", CommunityIdProcessor.Transport.Type.Tcp.getTransportNumber());
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testBeatsIanaNumberProtocolIPv4() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("iana_number", "4");
        network.remove("transport");
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("ip", "192.168.1.2");
        source.remove("port");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.put("ip", "10.1.2.3");
        destination.remove("port");
        testProcessor(event, "1:KXQzmk3bdsvD6UXj7dvQ4bM6Zvw=");
    }

    public void testIpv6() {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("ip", "2001:0db8:85a3:0000:0000:8a2e:0370:7334");
        @SuppressWarnings("unchecked")
        var destination = (Map<String, Object>) event.get("destination");
        destination.put("ip", "2001:0:9d38:6ab8:1c48:3a1c:a95a:b1c2");
        testProcessor(event, "1:YC1+javPJ2LpK5xVyw1udfT83Qs=");
    }

    public void testIcmpWithCodeEquivalent() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.put("transport", "icmp");
        var icmp = new HashMap<String, Object>();
        icmp.put("type", 10);
        icmp.put("code", 3);
        event.put("icmp", icmp);
        testProcessor(event, "1:L8wnzpmRHIESLqLBy+zTqW3Pmqs=");
    }

    public void testStringAndNumber() {
        // iana
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.remove("transport");
        network.put("iana_number", CommunityIdProcessor.Transport.Type.Tcp.getTransportNumber());
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        network.put("iana_number", Integer.toString(CommunityIdProcessor.Transport.Type.Tcp.getTransportNumber()));
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

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
        testProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");

        network2.put("transport", "2");
        testProcessor(event, "1:D3t8Q1aFA6Ev0A/AO4i9PnU3AeI=");

        // source port
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 34855);
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        source2.put("port", "34855");
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        // dest port
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var dest2 = (Map<String, Object>) event.get("destination");
        dest2.put("port", 80);
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        dest2.put("port", "80");
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");

        // icmp type and code
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var network3 = (Map<String, Object>) event.get("network");
        network3.put("transport", "icmp");
        var icmp = new HashMap<String, Object>();
        icmp.put("type", 3);
        icmp.put("code", 3);
        event.put("icmp", icmp);
        testProcessor(event, "1:KF3iG9XD24nhlSy4r1TcYIr5mfE=");

        icmp = new HashMap<>();
        icmp.put("type", "3");
        icmp.put("code", "3");
        event.put("icmp", icmp);
        testProcessor(event, "1:KF3iG9XD24nhlSy4r1TcYIr5mfE=");
    }

    public void testLongsForNumericValues() {
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 34855L);
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testFloatsForNumericValues() {
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 34855.0);
        testProcessor(event, "1:LQU9qZlK+B5F3KDmev6m5PMibrg=");
    }

    public void testInvalidPort() {
        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.put("port", 0);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid source port [0]"));

        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source2 = (Map<String, Object>) event.get("source");
        source2.put("port", 65536);
        e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid source port [65536]"));

        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source3 = (Map<String, Object>) event.get("destination");
        source3.put("port", 0);
        e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid destination port [0]"));

        event = buildEvent();
        @SuppressWarnings("unchecked")
        var source4 = (Map<String, Object>) event.get("destination");
        source4.put("port", 65536);
        e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, null));
        assertThat(e.getMessage(), containsString("invalid destination port [65536]"));
    }

    public void testIgnoreMissing() {
        @SuppressWarnings("unchecked")
        var network = (Map<String, Object>) event.get("network");
        network.remove("transport");
        testProcessor(event, 0, null, true);
    }

    public void testIgnoreMissingIsFalse() {
        @SuppressWarnings("unchecked")
        var source = (Map<String, Object>) event.get("source");
        source.remove("ip");

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> testProcessor(event, 0, null, false));

        assertThat(e.getMessage(), containsString("field [ip] not present as part of path [source.ip]"));
    }

    private static void testProcessor(Map<String, Object> source, String expectedHash) {
        testProcessor(source, 0, expectedHash);
    }

    private static void testProcessor(Map<String, Object> source, int seed, String expectedHash) {
        testProcessor(source, seed, expectedHash, false);
    }

    private static void testProcessor(Map<String, Object> source, int seed, String expectedHash, boolean ignoreMissing) {
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
        IngestDocument output;
        try {
            output = processor.execute(input);
        } catch (Exception e) {
            throw ExceptionsHelper.convertToRuntime(e);
        }

        String hash = output.getFieldValue(DEFAULT_TARGET, String.class, ignoreMissing);
        assertThat(hash, equalTo(expectedHash));
    }

    public void testTransportEnum() {
        for (CommunityIdProcessor.Transport.Type t : CommunityIdProcessor.Transport.Type.values()) {
            if (t == CommunityIdProcessor.Transport.Type.Unknown) {
                expectThrows(IllegalArgumentException.class, () -> CommunityIdProcessor.Transport.fromNumber(t.getTransportNumber()));
                continue;
            }

            assertThat(CommunityIdProcessor.Transport.fromNumber(t.getTransportNumber()).getType(), equalTo(t));
        }
    }

    public void testIcmpTypeEnum() {
        for (CommunityIdProcessor.IcmpType i : CommunityIdProcessor.IcmpType.values()) {
            assertThat(CommunityIdProcessor.IcmpType.fromNumber(i.getType()), equalTo(i));
        }
    }
}
