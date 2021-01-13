/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ingest.NetworkDirectionProcessor.Factory.DEFAULT_DEST_IP;
import static org.elasticsearch.xpack.ingest.NetworkDirectionProcessor.Factory.DEFAULT_SOURCE_IP;
import static org.elasticsearch.xpack.ingest.NetworkDirectionProcessor.Factory.DEFAULT_TARGET;
import static org.hamcrest.CoreMatchers.equalTo;

public class NetworkDirectionProcessorFactoryTests extends ESTestCase {

    private NetworkDirectionProcessor.Factory factory;

    @Before
    public void init() {
        factory = new NetworkDirectionProcessor.Factory();
    }

    public void testCreate() throws Exception {
        Map<String, Object> config = new HashMap<>();

        String sourceIpField = randomAlphaOfLength(6);
        config.put("source_ip", sourceIpField);
        String destIpField = randomAlphaOfLength(6);
        config.put("destination_ip", destIpField);
        String targetField = randomAlphaOfLength(6);
        config.put("target_field", targetField);
        List<String> internalNetworks = new ArrayList<>();
        internalNetworks.add("10.0.0.0/8");
        config.put("internal_networks", internalNetworks);
        boolean ignoreMissing = randomBoolean();
        config.put("ignore_missing", ignoreMissing);

        String processorTag = randomAlphaOfLength(10);
        NetworkDirectionProcessor networkProcessor = factory.create(null, processorTag, null, config);
        assertThat(networkProcessor.getTag(), equalTo(processorTag));
        assertThat(networkProcessor.getSourceIpField(), equalTo(sourceIpField));
        assertThat(networkProcessor.getDestinationIpField(), equalTo(destIpField));
        assertThat(networkProcessor.getTargetField(), equalTo(targetField));
        assertThat(networkProcessor.getInternalNetworks(), equalTo(internalNetworks));
        assertThat(networkProcessor.getIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testRequiredFields() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        try {
            factory.create(null, processorTag, null, config);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[internal_networks] required property is missing"));
        }
    }

    public void testDefaultFields() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        List<String> internalNetworks = new ArrayList<>();
        internalNetworks.add("10.0.0.0/8");
        config.put("internal_networks", internalNetworks);

        NetworkDirectionProcessor networkProcessor = factory.create(null, processorTag, null, config);
        assertThat(networkProcessor.getTag(), equalTo(processorTag));
        assertThat(networkProcessor.getSourceIpField(), equalTo(DEFAULT_SOURCE_IP));
        assertThat(networkProcessor.getDestinationIpField(), equalTo(DEFAULT_DEST_IP));
        assertThat(networkProcessor.getTargetField(), equalTo(DEFAULT_TARGET));
        assertThat(networkProcessor.getIgnoreMissing(), equalTo(true));
    }
}
