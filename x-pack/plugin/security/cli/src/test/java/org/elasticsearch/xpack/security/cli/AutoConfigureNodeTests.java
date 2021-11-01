/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.xpack.security.cli.AutoConfigureNode.removePreviousAutoconfiguration;

public class AutoConfigureNodeTests extends ESTestCase {

    public void testRemovePreviousAutoconfiguration() throws Exception {
        final List<String> file1 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line"
        );
        final List<String> file2 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            AutoConfigureNode.AUTO_CONFIGURATION_START_MARKER,
            "cluster.initial_master_nodes: [\"node1\"]",
            "http.host: [_site_]",
            "xpack.security.enabled: true",
            "xpack.security.enrollment.enabled: true",
            "xpack.security.http.ssl.enabled: true",
            "xpack.security.http.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.truststore.path: /path/to/the/file",
            AutoConfigureNode.AUTO_CONFIGURATION_END_MARKER
        );

        assertEquals(file1, removePreviousAutoconfiguration(file2));
    }

    public void testRemovePreviousAutoconfigurationRetainsUserAdded() throws Exception {
        final List<String> file1 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.extra.added.setting: value"
        );
        final List<String> file2 = List.of(
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            "some.setting1: some.value",
            "some.setting2: some.value",
            "some.setting3: some.value",
            "some.setting4: some.value",
            "# commented out line",
            "# commented out line",
            "# commented out line",
            AutoConfigureNode.AUTO_CONFIGURATION_START_MARKER,
            "cluster.initial_master_nodes: [\"node1\"]",
            "http.host: [_site_]",
            "xpack.security.enabled: true",
            "xpack.security.enrollment.enabled: true",
            "xpack.security.http.ssl.enabled: true",
            "some.extra.added.setting: value",
            "xpack.security.http.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.keystore.path: /path/to/the/file",
            "xpack.security.transport.ssl.truststore.path: /path/to/the/file",
            "",
            AutoConfigureNode.AUTO_CONFIGURATION_END_MARKER
        );
        assertEquals(file1, removePreviousAutoconfiguration(file2));
    }
}
