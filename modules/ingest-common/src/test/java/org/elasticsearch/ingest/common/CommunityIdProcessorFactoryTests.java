/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchException;
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
import static org.elasticsearch.ingest.common.CommunityIdProcessor.toUint16;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class CommunityIdProcessorFactoryTests extends ESTestCase {

    private CommunityIdProcessor.Factory factory;

    @Before
    public void init() {
        factory = new CommunityIdProcessor.Factory();
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();

        String sourceIpField = randomAlphaOfLength(6);
        config.put("source_ip", sourceIpField);
        String sourcePortField = randomAlphaOfLength(6);
        config.put("source_port", sourcePortField);
        String destIpField = randomAlphaOfLength(6);
        config.put("destination_ip", destIpField);
        String destPortField = randomAlphaOfLength(6);
        config.put("destination_port", destPortField);
        String ianaNumberField = randomAlphaOfLength(6);
        config.put("iana_number", ianaNumberField);
        String transportField = randomAlphaOfLength(6);
        config.put("transport", transportField);
        String icmpTypeField = randomAlphaOfLength(6);
        config.put("icmp_type", icmpTypeField);
        String icmpCodeField = randomAlphaOfLength(6);
        config.put("icmp_code", icmpCodeField);
        String targetField = randomAlphaOfLength(6);
        config.put("target_field", targetField);
        int seedInt = randomIntBetween(0, 65535);
        config.put("seed", Integer.toString(seedInt));
        boolean ignoreMissing = randomBoolean();
        config.put("ignore_missing", ignoreMissing);

        String processorTag = randomAlphaOfLength(10);
        CommunityIdProcessor communityIdProcessor = factory.create(null, processorTag, null, config);
        assertThat(communityIdProcessor.getTag(), equalTo(processorTag));
        assertThat(communityIdProcessor.getSourceIpField(), equalTo(sourceIpField));
        assertThat(communityIdProcessor.getSourcePortField(), equalTo(sourcePortField));
        assertThat(communityIdProcessor.getDestinationIpField(), equalTo(destIpField));
        assertThat(communityIdProcessor.getDestinationPortField(), equalTo(destPortField));
        assertThat(communityIdProcessor.getIanaNumberField(), equalTo(ianaNumberField));
        assertThat(communityIdProcessor.getTransportField(), equalTo(transportField));
        assertThat(communityIdProcessor.getIcmpTypeField(), equalTo(icmpTypeField));
        assertThat(communityIdProcessor.getIcmpCodeField(), equalTo(icmpCodeField));
        assertThat(communityIdProcessor.getTargetField(), equalTo(targetField));
        assertThat(communityIdProcessor.getSeed(), equalTo(toUint16(seedInt)));
        assertThat(communityIdProcessor.getIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testSeed() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);

        // negative seeds are rejected
        int tooSmallSeed = randomIntBetween(Integer.MIN_VALUE, -1);
        config.put("seed", Integer.toString(tooSmallSeed));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> factory.create(null, processorTag, null, config));
        assertThat(e.getMessage(), containsString("must be a value between 0 and 65535"));

        // seeds >= 2^16 are rejected
        int tooBigSeed = randomIntBetween(65536, Integer.MAX_VALUE);
        config.put("seed", Integer.toString(tooBigSeed));
        e = expectThrows(ElasticsearchException.class, () -> factory.create(null, processorTag, null, config));
        assertThat(e.getMessage(), containsString("must be a value between 0 and 65535"));

        // seeds between 0 and 2^16-1 are accepted
        int justRightSeed = randomIntBetween(0, 65535);
        byte[] expectedSeed = new byte[] { (byte) (justRightSeed >> 8), (byte) justRightSeed };
        config.put("seed", Integer.toString(justRightSeed));
        CommunityIdProcessor communityIdProcessor = factory.create(null, processorTag, null, config);
        assertThat(communityIdProcessor.getSeed(), equalTo(expectedSeed));
    }

    public void testRequiredFields() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        CommunityIdProcessor communityIdProcessor = factory.create(null, processorTag, null, config);
        assertThat(communityIdProcessor.getTag(), equalTo(processorTag));
        assertThat(communityIdProcessor.getSourceIpField(), equalTo(DEFAULT_SOURCE_IP));
        assertThat(communityIdProcessor.getSourcePortField(), equalTo(DEFAULT_SOURCE_PORT));
        assertThat(communityIdProcessor.getDestinationIpField(), equalTo(DEFAULT_DEST_IP));
        assertThat(communityIdProcessor.getDestinationPortField(), equalTo(DEFAULT_DEST_PORT));
        assertThat(communityIdProcessor.getIanaNumberField(), equalTo(DEFAULT_IANA_NUMBER));
        assertThat(communityIdProcessor.getTransportField(), equalTo(DEFAULT_TRANSPORT));
        assertThat(communityIdProcessor.getIcmpTypeField(), equalTo(DEFAULT_ICMP_TYPE));
        assertThat(communityIdProcessor.getIcmpCodeField(), equalTo(DEFAULT_ICMP_CODE));
        assertThat(communityIdProcessor.getTargetField(), equalTo(DEFAULT_TARGET));
        assertThat(communityIdProcessor.getSeed(), equalTo(toUint16(0)));
        assertThat(communityIdProcessor.getIgnoreMissing(), equalTo(true));
    }
}
