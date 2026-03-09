/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.seqno;

import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * Utilities for asserting on sequence number fields at the Lucene level in integration tests.
 */
public final class SequenceNumbersTestUtils {

    private SequenceNumbersTestUtils() {}

    /**
     * Asserts that all shards of the given index either have or lack {@code _seq_no} doc values on disk.
     *
     * @param indexName               the index to check
     * @param expectDocValuesOnDisk   {@code true} to assert doc values are present, {@code false} to assert they are empty
     * @param expectedShards          the exact number of shards expected to be verified
     */
    public static void assertShardsHaveSeqNoDocValues(String indexName, boolean expectDocValuesOnDisk, int expectedShards) {
        int nbCheckedShards = 0;
        for (var indicesServices : ESIntegTestCase.internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (var indexService : indicesServices) {
                if (indexService.index().getName().equals(indexName)) {
                    for (var indexShard : indexService) {
                        final var shardId = indexShard.shardId();
                        var checked = indexShard.withEngineOrNull(engine -> {
                            if (engine != null) {
                                try (var searcher = engine.acquireSearcher("assert_seq_no_dv")) {
                                    for (var leaf : searcher.getLeafContexts()) {
                                        var leafReader = leaf.reader();
                                        NumericDocValues seqNoDV = leafReader.getNumericDocValues(SeqNoFieldMapper.NAME);
                                        if (expectDocValuesOnDisk) {
                                            assertThat(shardId + " _seq_no doc values should be present", seqNoDV, notNullValue());
                                            assertThat(seqNoDV.nextDoc(), not(equalTo(DocIdSetIterator.NO_MORE_DOCS)));
                                        } else if (seqNoDV != null) {
                                            assertThat(
                                                shardId + " _seq_no doc values should be empty",
                                                seqNoDV.nextDoc(),
                                                equalTo(DocIdSetIterator.NO_MORE_DOCS)
                                            );
                                        }
                                        return true;
                                    }
                                } catch (IOException ioe) {
                                    throw new AssertionError(ioe);
                                }
                            }
                            return false;
                        });
                        if (checked) {
                            nbCheckedShards++;
                        }
                    }
                }
            }
        }
        assertThat("expected to verify " + expectedShards + " shard(s)", nbCheckedShards, equalTo(expectedShards));
    }
}
