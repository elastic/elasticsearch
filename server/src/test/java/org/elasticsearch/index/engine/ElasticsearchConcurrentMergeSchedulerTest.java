/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.engine;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchConcurrentMergeSchedulerTest extends ESTestCase {

    public void testDoMergeIgnoresAlreadyClosedException() throws Exception {
        String index = randomAlphaOfLength(8);
        ElasticsearchConcurrentMergeScheduler mergeScheduler = new ElasticsearchConcurrentMergeScheduler(
            new ShardId(index, UUIDs.randomBase64UUID(), randomInt(5)),
            new IndexSettings(IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), 1, 0)).build(), Settings.EMPTY)
        );

        var oneMerge = mock(MergePolicy.OneMerge.class);
        when(oneMerge.totalNumDocs()).thenReturn(randomIntBetween(10, 100));
        when(oneMerge.totalBytesSize()).thenReturn(randomLongBetween(1000, 10_000));
        var oneMergeProgress = mock(MergePolicy.OneMergeProgress.class);
        when(oneMergeProgress.getPauseTimes()).thenReturn(
            Map.of(
                MergePolicy.OneMergeProgress.PauseReason.STOPPED,
                randomLongBetween(0, 10),
                MergePolicy.OneMergeProgress.PauseReason.PAUSED,
                randomLongBetween(0, 10)
            )
        );
        when(oneMerge.getMergeProgress()).thenReturn(oneMergeProgress);

        var mergeSource = mock(MergeScheduler.MergeSource.class);
        doThrow(new AlreadyClosedException("Cache has been closed")).when(mergeSource).merge(oneMerge);

        mergeScheduler.doMerge(mergeSource, oneMerge);

        MergeStats stats = mergeScheduler.stats();
        assertEquals(1L, stats.getTotal());
        assertEquals(0L, stats.getCurrent());
        assertEquals(Set.of(), mergeScheduler.onGoingMerges());
    }
}
