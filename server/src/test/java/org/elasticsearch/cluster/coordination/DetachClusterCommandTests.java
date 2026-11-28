/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.terminal.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.ESTestCase.WithoutEntitlements;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;

@WithoutEntitlements // commands don't run with entitlements enforced
public class DetachClusterCommandTests extends ESTestCase {

    public void testDoesNotRenderClusterStateWhenNotVerbose() {
        final Metadata metadata = Metadata.builder().putCustom("throwing", new ThrowingCustomMetadataTest()).build();
        final ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        final MockTerminal terminal = MockTerminal.create();

        // Must not throw: at NORMAL verbosity the message is never built, so toString() is never called.
        DetachClusterCommand.printOldAndNewClusterStates(terminal, clusterState, clusterState);
    }

    public void testPrintsClusterStateWhenVerbose() {
        final Metadata metadata = Metadata.builder().putCustom("throwing", new ThrowingCustomMetadataTest()).build();
        final ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(metadata).build();

        final MockTerminal terminal = MockTerminal.create();
        terminal.setVerbosity(Terminal.Verbosity.VERBOSE);

        // At VERBOSE, the message is built, so toString() is called and throws.
        expectThrows(
            ClusterStateToStringCalled.class,
            () -> DetachClusterCommand.printOldAndNewClusterStates(terminal, clusterState, clusterState)
        );
    }

    private static class ThrowingCustomMetadataTest implements Metadata.ClusterCustom {

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }

        @Override
        public Diff<Metadata.ClusterCustom> diff(Metadata.ClusterCustom previousState) {
            return null;
        }

        @Override
        public EnumSet<Metadata.XContentContext> context() {
            return EnumSet.of(Metadata.XContentContext.GATEWAY);
        }

        @Override
        public String getWriteableName() {
            return null;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public String toString() {
            throw new ClusterStateToStringCalled();
        }
    }

    private static final class ClusterStateToStringCalled extends RuntimeException {
        private static final long serialVersionUID = 1L;

        ClusterStateToStringCalled() {
            super("ClusterState.toString() was called");
        }
    }
}
