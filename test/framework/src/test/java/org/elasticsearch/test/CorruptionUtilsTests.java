/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.stream.Stream;

import static org.elasticsearch.test.CorruptionUtils.corruptAt;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

public class CorruptionUtilsTests extends IndexShardTestCase {

    /**
     * There is a dependency on Lucene bug fix
     * https://github.com/elastic/elasticsearch/pull/33911
     */
    public void testLuceneCheckIndexIgnoresLast4Bytes() throws Exception {
        final IndexShard indexShard = newStartedShard(true);

        final long numDocs = between(10, 100);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Long.toString(i), "{}");
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final ShardPath shardPath = indexShard.shardPath();

        final Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);

        final Path cfsFile;
        try (Stream<Path> paths = Files.walk(indexPath)) {
            cfsFile = paths.filter(p -> p.getFileName().toString().endsWith(".cfs")).findFirst()
                .orElseThrow(() -> new IllegalStateException("CFS file has to be there"));
        }

        try (FileChannel raf = FileChannel.open(cfsFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            assertThat(raf.size(), lessThan(Integer.MAX_VALUE * 1L));
            final int maxPosition = (int) raf.size();
            // corrupt only last 4 bytes!
            final int position = randomIntBetween(maxPosition - 4, maxPosition - 1);
            corruptAt(cfsFile, raf, position);
        }

        final CheckIndex.Status status;
        try (CheckIndex checkIndex = new CheckIndex(new SimpleFSDirectory(indexPath))) {
            status = checkIndex.checkIndex();
        }

        assertThat("That's a good news! "
                + "Lucene now validates CRC32 of CFS file: time to drop workaround at CorruptionUtils (and this test)",
            status.clean, equalTo(true));
    }

    /**
     * There is a dependency on Lucene bug fix
     * https://issues.apache.org/jira/browse/LUCENE-8525
     *
     * {@link org.elasticsearch.index.store.Store#readSegmentsInfo(IndexCommit, Directory)}
     */
    public void testLuceneDetectsSegmentCorruptionDataAsIOException() throws Exception {
        final IndexShard indexShard = newStartedShard(true);

        // doesn't matter - we to generate segment file
        final int numDocs = randomIntBetween(10, 20);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Long.toString(i), "{}");
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final ShardPath shardPath = indexShard.shardPath();

        final Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);

        final Path segmentFile;
        try (Stream<Path> paths = Files.walk(indexPath)) {
            segmentFile = paths.filter(p -> p.getFileName().toString().startsWith("segments_")).findFirst()
                .orElseThrow(() -> new IllegalStateException("segment file has to be there"));
        }

        try (FileChannel raf = FileChannel.open(segmentFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // flipping a single byte at this position leads to reading numDVFields in SegmentInfos (while it was not there)
            // after that the following data structure is handled as Map<Integer,Set<String>>
            final int offset = 119;
            corruptAt(segmentFile, raf, offset, (byte)1); // numDVFields = 1

            // one of Set<String> has a negative size that triggers generic IOException
            for (int i = 0; i < 4; i++) {
                // numDVFields use a compressed byte as integer (1 byte)
                // integer is used for size of Set (4 bytes)
                corruptAt(segmentFile, raf, offset + Byte.BYTES + Integer.BYTES + i, (byte) -1);
            }
        }

        try (Directory directory = new SimpleFSDirectory(indexPath)) {
            final IOException exception = expectThrows(IOException.class, () -> Lucene.readSegmentInfos(directory));
            assertThat("Looks like the data structure is changed",
                exception.getMessage(), equalTo("Invalid vInt detected (too many bits)"));
            assertThat("Lucene has fixed handling data errors as smth more specific than IOException,"
                    + " time to clean up Store#readSegmentsInfo",
                exception.getClass(), equalTo(IOException.class));
        }
    }

    public void testLuceneFacesNegativeArraySize() throws Exception {
        final IndexShard indexShard = newStartedShard(true);

        // doesn't matter - we to generate segment file
        final int numDocs = randomIntBetween(10, 20);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, "_doc", Long.toString(i), "{}");
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final ShardPath shardPath = indexShard.shardPath();

        final Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);

        final Path segmentFile;
        try (Stream<Path> paths = Files.walk(indexPath)) {
            segmentFile = paths.filter(p -> p.getFileName().toString().startsWith("segments_")).findFirst()
                .orElseThrow(() -> new IllegalStateException("segment file has to be there"));
        }

        try (FileChannel raf = FileChannel.open(segmentFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            // flipping a single byte at this position leads to reading numDVFields in SegmentInfos (while it was not there)
            // after that the following data structure is handled as Map<Integer,Set<String>>
            final int offset = 119;
            corruptAt(segmentFile, raf, offset, (byte)1); // numDVFields = 1

            for (int i = 0; i < 4; i++) {
                // numDVFields use a compressed byte as integer (1 byte)
                // size of Set is a  compressed byte as integer (1 byte)
                // for the string size use all negatives
                corruptAt(segmentFile, raf, 125 + i, (byte) -1);
            }
            // and the last one is 0x0F that results to length of string = -1
            corruptAt(segmentFile, raf, 125 + 4, (byte) 0x0F);
        }

        try (Directory directory = new SimpleFSDirectory(indexPath)) {
            expectThrows(NegativeArraySizeException.class,
                "Lucene has fixed handling negative string sizes,"
                    + " time to clean up Store#readSegmentsInfo",
                () -> Lucene.readSegmentInfos(directory));
        }
    }
}
