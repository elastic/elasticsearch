/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory.Node;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.Version;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AbstractLocalClusterFactoryNodeTests {

    private static final DistributionResolver DISTRIBUTION_RESOLVER = (version, type) -> {
        throw new UnsupportedOperationException("Distribution resolution not required for tests");
    };

    @Test
    public void testIsAliveReturnsFalseWhenProcessIsNull() throws Exception {
        Node node = newNode("node-0");
        assertThat(node.isAlive(), is(false));
    }

    @Test
    public void testIsAliveReflectsProcessState() throws Exception {
        Node node = newNode("node-0");
        setProcess(node, new TestProcess(true));
        assertThat(node.isAlive(), is(true));

        setProcess(node, new TestProcess(false));
        assertThat(node.isAlive(), is(false));
    }

    private static Node newNode(String name) throws Exception {
        Path baseDir = Files.createTempDirectory("test-node");
        LocalClusterSpec clusterSpec = new LocalClusterSpec("cluster", List.of(User.DEFAULT_USER), List.of(), false);
        LocalNodeSpec nodeSpec = new LocalNodeSpec(
            clusterSpec,
            name,
            Version.CURRENT,
            List.of(),
            Map.of(),
            List.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            DistributionType.DEFAULT,
            Set.of(),
            List.of(),
            Map.of(),
            Map.of(),
            null,
            Map.of(),
            List.of(),
            Map.of(),
            List.of(),
            null
        );
        return new Node(baseDir, DISTRIBUTION_RESOLVER, nodeSpec);
    }

    private static void setProcess(Node node, Process process) throws Exception {
        Field processField = Node.class.getDeclaredField("process");
        processField.setAccessible(true);
        processField.set(node, process);
    }

    private static class TestProcess extends Process {
        private final boolean alive;

        TestProcess(boolean alive) {
            this.alive = alive;
        }

        @Override
        public OutputStream getOutputStream() {
            return OutputStream.nullOutputStream();
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public InputStream getErrorStream() {
            return new ByteArrayInputStream(new byte[0]);
        }

        @Override
        public int waitFor() {
            return 0;
        }

        @Override
        public int exitValue() {
            return 0;
        }

        @Override
        public void destroy() {}

        @Override
        public Process destroyForcibly() {
            return this;
        }

        @Override
        public boolean isAlive() {
            return alive;
        }
    }
}
