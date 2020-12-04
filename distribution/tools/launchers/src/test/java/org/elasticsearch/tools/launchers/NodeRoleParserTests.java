/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tools.launchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.DATA;
import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.MASTER_ONLY;
import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.ML_ONLY;
import static org.elasticsearch.tools.launchers.MachineDependentHeap.MachineNodeRole.UNKNOWN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

public class NodeRoleParserTests extends LaunchersTestCase {

    public void testMasterOnlyNode() {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("node.roles: [master]"));
        assertEquals(nodeRole, MASTER_ONLY);

        nodeRole = parseConfig(sb -> sb.append("node.roles: [master, voting_only]"));
        assertEquals(nodeRole, MASTER_ONLY);

        nodeRole = parseConfig(sb -> sb.append("node.roles: [master, some_other_role]"));
        assertNotEquals(nodeRole, MASTER_ONLY);
    }

    public void testMlOnlyNode() {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("node.roles: [ml]"));
        assertEquals(nodeRole, ML_ONLY);

        nodeRole = parseConfig(sb -> sb.append("node.roles: [ml, voting_only]"));
        assertEquals(nodeRole, ML_ONLY);

        nodeRole = parseConfig(sb -> sb.append("node.roles: [ml, some_other_role]"));
        assertNotEquals(nodeRole, ML_ONLY);
    }

    public void testDataNode() {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> {});
        assertEquals(nodeRole, DATA);

        nodeRole = parseConfig(sb -> sb.append("node.roles: []"));
        assertEquals(nodeRole, DATA);

        nodeRole = parseConfig(sb -> sb.append("node.roles: [some_unknown_role]"));
        assertEquals(nodeRole, DATA);

        nodeRole = parseConfig(sb -> sb.append("node.roles: [master, ingest]"));
        assertEquals(nodeRole, DATA);

        nodeRole = parseConfig(sb -> sb.append("node.roles: [ml, master]"));
        assertEquals(nodeRole, DATA);
    }

    public void testLegacySettings() {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> sb.append("node.ml: true"));
        assertEquals(nodeRole, UNKNOWN);

        nodeRole = parseConfig(sb -> sb.append("node.master: true"));
        assertEquals(nodeRole, UNKNOWN);

        nodeRole = parseConfig(sb -> sb.append("node.data: false"));
        assertEquals(nodeRole, UNKNOWN);

        nodeRole = parseConfig(sb -> sb.append("node.ingest: false"));
        assertEquals(nodeRole, UNKNOWN);
    }

    public void testYamlSyntax() {
        MachineDependentHeap.MachineNodeRole nodeRole = parseConfig(sb -> {
            sb.append("node:\n");
            sb.append("  roles:\n");
            sb.append("    - master");
        });
        assertEquals(nodeRole, MASTER_ONLY);

        nodeRole = parseConfig(sb -> {
            sb.append("node:\n");
            sb.append("  roles: [ml]");
        });
        assertEquals(nodeRole, ML_ONLY);
    }

    public void testInvalidRoleSyntax() {
        try {
            parseConfig(sb -> sb.append("node.roles: foo"));
            fail("expected config parse exception");
        } catch (IllegalStateException expected) {
            assertEquals(expected.getMessage(), "Unable to parse elasticsearch.yml. Expected 'node.roles' to be a list.");
        }
    }

    private static MachineDependentHeap.MachineNodeRole parseConfig(Consumer<StringBuilder> action) {
        StringBuilder sb = new StringBuilder();
        action.accept(sb);

        try (InputStream config = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8))) {
            return MachineDependentHeap.NodeRoleParser.parse(config);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
