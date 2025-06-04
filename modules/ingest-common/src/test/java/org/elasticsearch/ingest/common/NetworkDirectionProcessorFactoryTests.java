/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.common.NetworkDirectionProcessor.Factory.DEFAULT_DEST_IP;
import static org.elasticsearch.ingest.common.NetworkDirectionProcessor.Factory.DEFAULT_SOURCE_IP;
import static org.elasticsearch.ingest.common.NetworkDirectionProcessor.Factory.DEFAULT_TARGET;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class NetworkDirectionProcessorFactoryTests extends ESTestCase {

    private NetworkDirectionProcessor.Factory factory;

    @Before
    public void init() {
        factory = new NetworkDirectionProcessor.Factory(TestTemplateService.instance());
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
        NetworkDirectionProcessor networkProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(networkProcessor.getTag(), equalTo(processorTag));
        assertThat(networkProcessor.getSourceIpField(), equalTo(sourceIpField));
        assertThat(networkProcessor.getDestinationIpField(), equalTo(destIpField));
        assertThat(networkProcessor.getTargetField(), equalTo(targetField));
        assertThat(networkProcessor.getInternalNetworks().size(), greaterThan(0));
        assertThat(networkProcessor.getInternalNetworks().get(0).newInstance(Map.of()).execute(), equalTo("10.0.0.0/8"));
        assertThat(networkProcessor.getIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testCreateInternalNetworksField() throws Exception {
        Map<String, Object> config = new HashMap<>();

        String sourceIpField = randomAlphaOfLength(6);
        config.put("source_ip", sourceIpField);
        String destIpField = randomAlphaOfLength(6);
        config.put("destination_ip", destIpField);
        String targetField = randomAlphaOfLength(6);
        config.put("target_field", targetField);
        String internalNetworksField = randomAlphaOfLength(6);
        config.put("internal_networks_field", internalNetworksField);
        boolean ignoreMissing = randomBoolean();
        config.put("ignore_missing", ignoreMissing);

        String processorTag = randomAlphaOfLength(10);
        NetworkDirectionProcessor networkProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(networkProcessor.getTag(), equalTo(processorTag));
        assertThat(networkProcessor.getSourceIpField(), equalTo(sourceIpField));
        assertThat(networkProcessor.getDestinationIpField(), equalTo(destIpField));
        assertThat(networkProcessor.getTargetField(), equalTo(targetField));
        assertThat(networkProcessor.getInternalNetworksField(), equalTo(internalNetworksField));
        assertThat(networkProcessor.getIgnoreMissing(), equalTo(ignoreMissing));
    }

    public void testRequiredFields() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        try {
            factory.create(null, processorTag, null, config, null);
            fail("factory create should have failed");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[internal_networks] or [internal_networks_field] must be specified"));
        }
    }

    public void testDefaultFields() throws Exception {
        HashMap<String, Object> config = new HashMap<>();
        String processorTag = randomAlphaOfLength(10);
        List<String> internalNetworks = new ArrayList<>();
        internalNetworks.add("10.0.0.0/8");
        config.put("internal_networks", internalNetworks);

        NetworkDirectionProcessor networkProcessor = factory.create(null, processorTag, null, config, null);
        assertThat(networkProcessor.getTag(), equalTo(processorTag));
        assertThat(networkProcessor.getSourceIpField(), equalTo(DEFAULT_SOURCE_IP));
        assertThat(networkProcessor.getDestinationIpField(), equalTo(DEFAULT_DEST_IP));
        assertThat(networkProcessor.getTargetField(), equalTo(DEFAULT_TARGET));
        assertThat(networkProcessor.getIgnoreMissing(), equalTo(true));
    }
}
