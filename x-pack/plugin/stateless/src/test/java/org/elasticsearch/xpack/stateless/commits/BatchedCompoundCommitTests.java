/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.util.List;

import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomCompoundCommit;
import static org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommitTestUtils.randomShardId;
import static org.hamcrest.Matchers.equalTo;

public class BatchedCompoundCommitTests extends ESTestCase {

    public void testToStringUsesFirstCompoundCommitGeneration() {
        var shardId = randomShardId();
        var primaryTerm = randomLongBetween(1L, Long.MAX_VALUE - 1L);
        var first = randomCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, 5), false);
        var second = randomCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, 6), false);
        var third = randomCompoundCommit(shardId, new PrimaryTermAndGeneration(primaryTerm, 7), true);

        var batchedCompoundCommit = new BatchedCompoundCommit(
            first.primaryTermAndGeneration(),
            List.of(first, second, third)
        );

        assertThat(batchedCompoundCommit.toBlobFile().blobName(), equalTo("stateless_commit_5"));
        assertThat(batchedCompoundCommit.toString(), equalTo("[stateless_commit_5][term:" + primaryTerm +"][gen:7][h]"));
    }
}
