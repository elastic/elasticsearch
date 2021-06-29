/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tools.launchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.DATA;
import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.MASTER_ONLY;
import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.ML_ONLY;
import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.UNKNOWN;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class NodeRoleParserTests extends LaunchersTestCase {

    public void testMasterOnlyNode() throws IOException {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("node.roles: [master]"));
        assertThat(nodeRole, equalTo(MASTER_ONLY));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [master, some_other_role]"));
        assertThat(nodeRole, not(equalTo(MASTER_ONLY)));
    }

    public void testMlOnlyNode() throws IOException {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("node.roles: [ml]"));
        assertThat(nodeRole, equalTo(ML_ONLY));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [ml, remote_cluster_client]"));
        assertThat(nodeRole, equalTo(ML_ONLY));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [remote_cluster_client, ml]"));
        assertThat(nodeRole, equalTo(ML_ONLY));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [remote_cluster_client]"));
        assertThat(nodeRole, not(equalTo(ML_ONLY)));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [ml, some_other_role]"));
        assertThat(nodeRole, not(equalTo(ML_ONLY)));
    }

    public void testDataNode() throws IOException {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> {});
        assertThat(nodeRole, equalTo(DATA));

        nodeRole = parseConfig(sb -> sb.append("node.roles: []"));
        assertThat(nodeRole, equalTo(DATA));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [some_unknown_role]"));
        assertThat(nodeRole, equalTo(DATA));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [master, ingest]"));
        assertThat(nodeRole, equalTo(DATA));

        nodeRole = parseConfig(sb -> sb.append("node.roles: [ml, master]"));
        assertThat(nodeRole, equalTo(DATA));
    }

    public void testLegacySettings() throws IOException {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("node.ml: true"));
        assertThat(nodeRole, equalTo(UNKNOWN));

        nodeRole = parseConfig(sb -> sb.append("node.master: true"));
        assertThat(nodeRole, equalTo(UNKNOWN));

        nodeRole = parseConfig(sb -> sb.append("node.data: false"));
        assertThat(nodeRole, equalTo(UNKNOWN));

        nodeRole = parseConfig(sb -> sb.append("node.ingest: false"));
        assertThat(nodeRole, equalTo(UNKNOWN));
    }

    public void testYamlSyntax() throws IOException {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> {
            sb.append("node:\n");
            sb.append("  roles:\n");
            sb.append("    - master");
        });
        assertThat(nodeRole, equalTo(MASTER_ONLY));

        nodeRole = parseConfig(sb -> {
            sb.append("node:\n");
            sb.append("  roles: [ml]");
        });
        assertThat(nodeRole, equalTo(ML_ONLY));
    }

    public void testInvalidYaml() throws IOException {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("notyaml"));
        assertThat(nodeRole, equalTo(UNKNOWN));
    }

    public void testInvalidRoleSyntax() throws IOException {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("node.roles: foo"));
        assertThat(nodeRole, equalTo(UNKNOWN));
    }

    private static MachineDependentHeap.MachineNodeRole parseConfig(Consumer<StringBuilder> action) throws IOException {
        StringBuilder sb = new StringBuilder();
        action.accept(sb);

        try (InputStream config = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8))) {
            return MachineDependentHeap.NodeRoleParser.parse(config);
        }
    }
}
