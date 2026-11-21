/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.parquet.write.table.TableReader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** A search replica serves the files of its own shard, of every history, in the order of the histories and the sequence numbers. */
public class SearchReplicaFilesTests extends ESTestCase {

    private static final String ROOT = "s3://bucket/logs/uuid";
    private static final ShardId SHARD = new ShardId(new Index("logs", "uuid"), 1);

    private static TableReader.DataFileEntry file(int shard, String history, long from, long to) {
        return new TableReader.DataFileEntry(
            ROOT + String.format(java.util.Locale.ROOT, "/data/shard-%d/%s/%019d-%019d-attempt.parquet", shard, history, from, to),
            1000,
            to - from + 1
        );
    }

    public void testTheFilesOfTheShardInTheOrderOfHistoryAndSequenceNumber() {
        final List<TableReader.DataFileEntry> mine = List.of(
            file(1, "history-a", 0, 99),
            file(1, "history-a", 100, 199),
            file(1, "history-a", 200, 1099),
            file(1, "history-b", 0, 49)
        );
        final List<TableReader.DataFileEntry> all = new ArrayList<>(mine);
        all.add(file(0, "history-a", 0, 99));
        all.add(file(2, "history-c", 0, 99));
        // not a file of the write engine
        all.add(new TableReader.DataFileEntry(ROOT + "/data/other.parquet", 10, 1));
        Collections.shuffle(all, random());

        final List<TableReader.DataFileEntry> selected = ParquetWriteEnginePlugin.filesOfShard(
            SHARD,
            new TableReader.Contents(7, Map.of("elasticsearch.index_uuid", "uuid"), all)
        );
        assertThat(selected, equalTo(mine));
    }

    public void testATableOfAnotherIndexIsRefused() {
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ParquetWriteEnginePlugin.filesOfShard(
                SHARD,
                new TableReader.Contents(7, Map.of("elasticsearch.index_uuid", "other"), List.of(file(1, "h", 0, 9)))
            )
        );
        assertThat(e.getMessage(), containsString("belongs to the index [other]"));
    }
}
