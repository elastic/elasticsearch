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
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardPath;

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
}
