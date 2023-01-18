/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.common.NetworkDirectionProcessor.Factory.DEFAULT_TARGET;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class NetworkDirectionProcessorTests extends ESTestCase {
    private Map<String, Object> buildEvent() {
        return buildEvent("128.232.110.120");
    }

    private Map<String, Object> buildEvent(String source) {
        return buildEvent(source, "66.35.250.204");
    }

    private Map<String, Object> buildEvent(String source, String destination) {
        return new HashMap<>() {
            {
                put("source", new HashMap<String, Object>() {
                    {
                        put("ip", source);
                    }
                });
                put("destination", new HashMap<String, Object>() {
                    {
                        put("ip", destination);
                    }
                });
            }
        };
    }

    public void testNoInternalNetworks() throws Exception {
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> testNetworkDirectionProcessor(buildEvent(), null)
        );
        assertThat(e.getMessage(), containsString("[internal_networks] or [internal_networks_field] must be specified"));
    }

    public void testNoSource() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testNetworkDirectionProcessor(buildEvent(null), new String[] { "10.0.0.0/8" })
        );
        assertThat(e.getMessage(), containsString("unable to calculate network direction from document"));
    }

    public void testNoDestination() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testNetworkDirectionProcessor(buildEvent("192.168.1.1", null), new String[] { "10.0.0.0/8" })
        );
        assertThat(e.getMessage(), containsString("unable to calculate network direction from document"));
    }

    public void testIgnoreMissing() throws Exception {
        testNetworkDirectionProcessor(buildEvent(null), new String[] { "10.0.0.0/8" }, null, true);
    }

    public void testInvalidSource() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testNetworkDirectionProcessor(buildEvent("invalid"), new String[] { "10.0.0.0/8" })
        );
        assertThat(e.getMessage(), containsString("'invalid' is not an IP string literal."));
    }

    public void testInvalidDestination() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testNetworkDirectionProcessor(buildEvent("192.168.1.1", "invalid"), new String[] { "10.0.0.0/8" })
        );
        assertThat(e.getMessage(), containsString("'invalid' is not an IP string literal."));
    }

    public void testInvalidNetwork() throws Exception {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> testNetworkDirectionProcessor(buildEvent("192.168.1.1", "192.168.1.1"), new String[] { "10.0.0.0/8", "invalid" })
        );
        assertThat(e.getMessage(), containsString("'invalid' is not an IP string literal."));
    }

    public void testCIDR() throws Exception {
        testNetworkDirectionProcessor(buildEvent("10.0.1.1", "192.168.1.2"), new String[] { "10.0.0.0/8" }, "outbound");
        testNetworkDirectionProcessor(buildEvent("192.168.1.2", "10.0.1.1"), new String[] { "10.0.0.0/8" }, "inbound");
    }

    public void testUnspecified() throws Exception {
        testNetworkDirectionProcessor(buildEvent("0.0.0.0", "0.0.0.0"), new String[] { "unspecified" }, "internal");
        testNetworkDirectionProcessor(buildEvent("::", "::"), new String[] { "unspecified" }, "internal");
    }

    public void testNetworkPrivate() throws Exception {
        testNetworkDirectionProcessor(buildEvent("192.168.1.1", "192.168.1.2"), new String[] { "private" }, "internal");
        testNetworkDirectionProcessor(buildEvent("10.0.1.1", "192.168.1.2"), new String[] { "private" }, "internal");
        testNetworkDirectionProcessor(buildEvent("192.168.1.1", "172.16.0.1"), new String[] { "private" }, "internal");
        testNetworkDirectionProcessor(buildEvent("192.168.1.1", "fd12:3456:789a:1::1"), new String[] { "private" }, "internal");
    }

    public void testNetworkPublic() throws Exception {
        testNetworkDirectionProcessor(buildEvent("192.168.1.1", "192.168.1.2"), new String[] { "public" }, "external");
        testNetworkDirectionProcessor(buildEvent("10.0.1.1", "192.168.1.2"), new String[] { "public" }, "external");
        testNetworkDirectionProcessor(buildEvent("192.168.1.1", "172.16.0.1"), new String[] { "public" }, "external");
        testNetworkDirectionProcessor(buildEvent("192.168.1.1", "fd12:3456:789a:1::1"), new String[] { "public" }, "external");
    }

    private void testNetworkDirectionProcessor(Map<String, Object> source, String[] internalNetworks) throws Exception {
        testNetworkDirectionProcessor(source, internalNetworks, "");
    }

    private void testNetworkDirectionProcessor(Map<String, Object> source, String[] internalNetworks, String expectedDirection)
        throws Exception {
        testNetworkDirectionProcessor(source, internalNetworks, expectedDirection, false);
    }

    public void testReadFromField() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        Map<String, Object> source = buildEvent("192.168.1.1", "192.168.1.2");
        ArrayList<String> networks = new ArrayList<>();
        networks.add("public");
        source.put("some_field", networks);

        Map<String, Object> config = new HashMap<>();
        config.put("internal_networks_field", "some_field");
        NetworkDirectionProcessor processor = new NetworkDirectionProcessor.Factory(TestTemplateService.instance()).create(
            null,
            processorTag,
            null,
            config
        );
        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        IngestDocument output = processor.execute(input);
        String hash = output.getFieldValue(DEFAULT_TARGET, String.class);
        assertThat(hash, equalTo("external"));
    }

    public void testInternalNetworksAndField() throws Exception {
        String processorTag = randomAlphaOfLength(10);
        Map<String, Object> source = buildEvent("192.168.1.1", "192.168.1.2");
        ArrayList<String> networks = new ArrayList<>();
        networks.add("public");
        source.put("some_field", networks);
        Map<String, Object> config = new HashMap<>();
        config.put("internal_networks_field", "some_field");
        config.put("internal_networks", networks);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> new NetworkDirectionProcessor.Factory(TestTemplateService.instance()).create(null, processorTag, null, config)
        );
        assertThat(
            e.getMessage(),
            containsString("[internal_networks] and [internal_networks_field] cannot both be used in the same processor")
        );
    }

    private void testNetworkDirectionProcessor(
        Map<String, Object> source,
        String[] internalNetworks,
        String expectedDirection,
        boolean ignoreMissing
    ) throws Exception {
        List<String> networks = null;

        if (internalNetworks != null) networks = List.of(internalNetworks);

        String processorTag = randomAlphaOfLength(10);
        Map<String, Object> config = new HashMap<>();
        config.put("internal_networks", networks);
        config.put("ignore_missing", ignoreMissing);
        NetworkDirectionProcessor processor = new NetworkDirectionProcessor.Factory(TestTemplateService.instance()).create(
            null,
            processorTag,
            null,
            config
        );

        IngestDocument input = TestIngestDocument.withDefaultVersion(source);
        IngestDocument output = processor.execute(input);

        String hash = output.getFieldValue(DEFAULT_TARGET, String.class, ignoreMissing);
        assertThat(hash, equalTo(expectedDirection));
    }
}
