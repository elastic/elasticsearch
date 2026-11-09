/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.HollowCommitMetadata;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

/**
 * The final uploaded hollow commit and the metadata of its Lucene commit, carried by primary relocation so the target can open its
 * hollow engine without object-store reads.
 */
public record HollowShardRecoveryState(BatchedCompoundCommit commit, HollowCommitMetadata metadata) implements Writeable {

    public HollowShardRecoveryState {
        if (matches(commit, metadata) == false) {
            throw new IllegalArgumentException("hollow commit metadata [" + metadata + "] must match the hollow commit " + commit);
        }
    }

    public HollowShardRecoveryState(StreamInput in) throws IOException {
        this(readCommit(in), in);
    }

    private HollowShardRecoveryState(BatchedCompoundCommit commit, StreamInput in) throws IOException {
        this(commit, new HollowCommitMetadata(in, commit.lastCompoundCommit().commitFiles().keySet()));
    }

    private static BatchedCompoundCommit readCommit(StreamInput in) throws IOException {
        return new BatchedCompoundCommit(
            new PrimaryTermAndGeneration(in),
            in.readCollectionAsList(StatelessCompoundCommit::readFromTransport)
        );
    }

    /**
     * Returns whether the metadata describes the last commit of the given batched compound commit, which must be hollow.
     */
    public static boolean matches(BatchedCompoundCommit commit, HollowCommitMetadata metadata) {
        final var lastCommit = commit.lastCompoundCommit();
        return lastCommit.hollow()
            && lastCommit.generation() == metadata.generation()
            && lastCommit.commitFiles().keySet().equals(metadata.files());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        commit.primaryTermAndGeneration().writeTo(out);
        out.writeCollection(commit.compoundCommits());
        metadata.writeTo(out);
    }
}
