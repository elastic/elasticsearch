/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

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

        String tag = randomAlphaOfLength(10);
        CommunityIdProcessor processor = factory.create(null, tag, null, config, null);
        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getSourceIpField(), equalTo(sourceIpField));
        assertThat(processor.getSourcePortField(), equalTo(sourcePortField));
        assertThat(processor.getDestinationIpField(), equalTo(destIpField));
        assertThat(processor.getDestinationPortField(), equalTo(destPortField));
        assertThat(processor.getIanaNumberField(), equalTo(ianaNumberField));
        assertThat(processor.getTransportField(), equalTo(transportField));
        assertThat(processor.getIcmpTypeField(), equalTo(icmpTypeField));
        assertThat(processor.getIcmpCodeField(), equalTo(icmpCodeField));
        assertThat(processor.getTargetField(), equalTo(targetField));
        assertThat(processor.getSeed(), equalTo(toUint16(seedInt)));
        assertThat(processor.getIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testSeed() throws Exception {
        Map<String, Object> config = new HashMap<>();
        String tag = randomAlphaOfLength(10);

        // negative seeds are rejected
        int tooSmallSeed = randomIntBetween(Integer.MIN_VALUE, -1);
        config.put("seed", Integer.toString(tooSmallSeed));
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> factory.create(null, tag, null, config, null));
        assertThat(e.getMessage(), containsString("must be a value between 0 and 65535"));

        // seeds >= 2^16 are rejected
        int tooBigSeed = randomIntBetween(65536, Integer.MAX_VALUE);
        config.put("seed", Integer.toString(tooBigSeed));
        e = expectThrows(ElasticsearchException.class, () -> factory.create(null, tag, null, config, null));
        assertThat(e.getMessage(), containsString("must be a value between 0 and 65535"));

        // seeds between 0 and 2^16-1 are accepted
        int justRightSeed = randomIntBetween(0, 65535);
        byte[] expectedSeed = new byte[] { (byte) (justRightSeed >> 8), (byte) justRightSeed };
        config.put("seed", Integer.toString(justRightSeed));
        CommunityIdProcessor processor = factory.create(null, tag, null, config, null);
        assertThat(processor.getSeed(), equalTo(expectedSeed));
    }

    public void testRequiredFields() throws Exception {
        String tag = randomAlphaOfLength(10);
        CommunityIdProcessor processor = factory.create(null, tag, null, new HashMap<>(), null);
        assertThat(processor.getTag(), equalTo(tag));
        assertThat(processor.getSourceIpField(), equalTo(DEFAULT_SOURCE_IP));
        assertThat(processor.getSourcePortField(), equalTo(DEFAULT_SOURCE_PORT));
        assertThat(processor.getDestinationIpField(), equalTo(DEFAULT_DEST_IP));
        assertThat(processor.getDestinationPortField(), equalTo(DEFAULT_DEST_PORT));
        assertThat(processor.getIanaNumberField(), equalTo(DEFAULT_IANA_NUMBER));
        assertThat(processor.getTransportField(), equalTo(DEFAULT_TRANSPORT));
        assertThat(processor.getIcmpTypeField(), equalTo(DEFAULT_ICMP_TYPE));
        assertThat(processor.getIcmpCodeField(), equalTo(DEFAULT_ICMP_CODE));
        assertThat(processor.getTargetField(), equalTo(DEFAULT_TARGET));
        assertThat(processor.getSeed(), equalTo(toUint16(0)));
        assertThat(processor.getIgnoreMissing(), equalTo(true));
    }
}
