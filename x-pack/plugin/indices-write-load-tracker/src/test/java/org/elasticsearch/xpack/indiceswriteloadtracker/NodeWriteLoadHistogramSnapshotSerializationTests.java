/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.indiceswriteloadtracker;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xpack.indiceswriteloadtracker.HistogramSnapshotSerializationTests.mutateHistogramSnapshot;
import static org.elasticsearch.xpack.indiceswriteloadtracker.HistogramSnapshotSerializationTests.randomHistogramSnapshot;

public class NodeWriteLoadHistogramSnapshotSerializationTests extends AbstractSerializingTestCase<NodeWriteLoadHistogramSnapshot> {
    @Override
    protected NodeWriteLoadHistogramSnapshot doParseInstance(XContentParser parser) throws IOException {
        return NodeWriteLoadHistogramSnapshot.parse(parser);
    }

    @Override
    protected Writeable.Reader<NodeWriteLoadHistogramSnapshot> instanceReader() {
        return NodeWriteLoadHistogramSnapshot::new;
    }

    @Override
    protected NodeWriteLoadHistogramSnapshot createTestInstance() {
        return new NodeWriteLoadHistogramSnapshot(
            randomAlphaOfLength(10),
            new WriteLoadHistogramSnapshot(
                randomNonNegativeLong(),
                randomHistogramSnapshot(),
                randomHistogramSnapshot(),
                randomHistogramSnapshot()
            )
        );
    }

    @Override
    protected NodeWriteLoadHistogramSnapshot mutateInstance(NodeWriteLoadHistogramSnapshot instance) throws IOException {
        final var mutationBranch = randomInt(4);
        return switch (mutationBranch) {
            case 0 -> new NodeWriteLoadHistogramSnapshot(randomAlphaOfLength(10), instance.writeLoadHistogramSnapshot());
            case 1 -> new NodeWriteLoadHistogramSnapshot(
                instance.nodeId(),
                new WriteLoadHistogramSnapshot(
                    randomNonNegativeLong(),
                    instance.indexLoadHistogramSnapshot(),
                    instance.mergeLoadHistogramSnapshot(),
                    instance.refreshLoadHistogramSnapshot()
                )
            );
            case 2 -> new NodeWriteLoadHistogramSnapshot(
                instance.nodeId(),
                new WriteLoadHistogramSnapshot(
                    instance.timestamp(),
                    mutateHistogramSnapshot(instance.indexLoadHistogramSnapshot()),
                    instance.mergeLoadHistogramSnapshot(),
                    instance.refreshLoadHistogramSnapshot()
                )
            );
            case 3 -> new NodeWriteLoadHistogramSnapshot(
                instance.nodeId(),
                new WriteLoadHistogramSnapshot(
                    instance.timestamp(),
                    instance.indexLoadHistogramSnapshot(),
                    mutateHistogramSnapshot(instance.mergeLoadHistogramSnapshot()),
                    instance.refreshLoadHistogramSnapshot()
                )
            );
            case 4 -> new NodeWriteLoadHistogramSnapshot(
                instance.nodeId(),
                new WriteLoadHistogramSnapshot(
                    instance.timestamp(),
                    instance.indexLoadHistogramSnapshot(),
                    instance.mergeLoadHistogramSnapshot(),
                    mutateHistogramSnapshot(instance.refreshLoadHistogramSnapshot())
                )
            );
            default -> throw new IllegalStateException("Unexpected value: " + mutationBranch);
        };
    }
}
