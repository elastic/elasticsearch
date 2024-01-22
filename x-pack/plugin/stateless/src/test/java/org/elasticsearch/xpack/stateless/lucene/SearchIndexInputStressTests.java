/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.apache.lucene.codecs.CodecUtil;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class SearchIndexInputStressTests extends ESIndexInputTestCase {

    public void testConcurrentReadsUnderHighContention() throws InterruptedException, IOException {
        final TestThreadPool threadPool = new TestThreadPool(
            "testConcurrentReadsWithMultipleSearchIndexInput",
            Stateless.statelessExecutorBuilders(Settings.EMPTY, true)
        );

        // The shared cache write buffer size is set to 8kb in build.gradle.kts
        // This also means the test must run with Gradle test runner
        final int writerBufferSize = 8 * 1024;
        // We use a region size of [64kb, 128kb] to ensure filling the region requires multiple writes
        assertThat(SharedBytes.MAX_BYTES_PER_WRITE, equalTo(writerBufferSize));
        final int regionSize = between(8, 16) * writerBufferSize;
        // Chose a smaller number of regions for high contention (eviction rate)
        final int numberOfRegions = between(1, 3);
        final var settings = sharedCacheSettings(
            ByteSizeValue.ofBytes((long) regionSize * numberOfRegions),
            ByteSizeValue.ofBytes(regionSize)
        );

        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            SharedBlobCacheService<FileCacheKey> sharedBlobCacheService = new SharedBlobCacheService<>(
                nodeEnvironment,
                settings,
                threadPool,
                Stateless.SHARD_READ_THREAD_POOL,
                BlobCacheMetrics.NOOP
            )
        ) {
            final Map<SearchIndexInput, String> searchIndexInputs = new HashMap<>();
            final int numberOfCompoundFiles = between(10, 15);
            for (int i = 0; i < numberOfCompoundFiles; i++) {
                searchIndexInputs.putAll(generateSearchIndexInputsForOneCompoundFile(sharedBlobCacheService, regionSize));
            }

            final int numberOfReaders = between(10, 15);
            final int readsPerReader = between(30, 50);

            // Prepare the queue of searchIndexInput in the main thread to increase randomisation
            final Queue<Map.Entry<SearchIndexInput, String>> queue = new ConcurrentLinkedQueue<>(
                IntStream.range(0, numberOfReaders * readsPerReader).mapToObj(ignore -> randomFrom(searchIndexInputs.entrySet())).toList()
            );

            final CyclicBarrier cyclicBarrier = new CyclicBarrier(numberOfReaders + 1);
            final List<Thread> threads = IntStream.range(0, numberOfReaders).mapToObj(ignore -> {
                final Thread thread = new Thread(() -> {
                    safeAwait(cyclicBarrier);
                    for (int i = 0; i < readsPerReader; i++) {
                        final Map.Entry<SearchIndexInput, String> entry = queue.poll();
                        assertThat(entry, notNullValue());
                        final SearchIndexInput searchIndexInput = entry.getKey();

                        final String calculatedChecksum;
                        try {
                            calculatedChecksum = Store.digestToString(CodecUtil.checksumEntireFile(searchIndexInput));
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                        assertThat("checksum mismatch for " + searchIndexInput, calculatedChecksum, equalTo(entry.getValue()));
                    }
                });
                thread.start();
                return thread;
            }).toList();

            safeAwait(cyclicBarrier);
            for (Thread thread : threads) {
                thread.join();
            }
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    private Map<SearchIndexInput, String> generateSearchIndexInputsForOneCompoundFile(
        SharedBlobCacheService<FileCacheKey> sharedBlobCacheService,
        int regionSize
    ) throws IOException {
        byte[] allBytes = null;
        final List<ChecksumAndLength> checksumAndLengthList = new ArrayList<>();
        final int numberOfEmbeddedFiles = between(3, 5);

        // SharedBytes#copyToCacheFileAligned attempts to release the read side every time SharedBytes.MAX_BYTES_PER_WRITE
        // bytes are read while the write side keeps going till the region is filled.
        // We randomly generate files that can be very small (a few bytes) or large (double the region size)
        // to mix up the file's reading time and check that the code performs correctly.
        // In actual compound commit files, Lucene bloom-filter file can be an example of small files
        // and Lucene compound segment file (.cfs) can be an example of large files.
        for (int i = 0; i < numberOfEmbeddedFiles; i++) {
            final Tuple<String, byte[]> checksumBytes = randomChecksumBytes(between(1, regionSize * 2));
            if (allBytes == null) {
                allBytes = checksumBytes.v2();
            } else {
                allBytes = concatBytes(allBytes, checksumBytes.v2());
            }
            checksumAndLengthList.add(new ChecksumAndLength(checksumBytes.v1(), checksumBytes.v2().length));
        }

        final long primaryTerm = randomNonNegativeLong();
        final String compoundFileName = StatelessCompoundCommit.blobNameFromGeneration(primaryTerm);
        long offset = 0;
        final Map<SearchIndexInput, String> searchIndexInputs = new HashMap<>();
        for (ChecksumAndLength checksumAndLength : checksumAndLengthList) {
            final String fileName = randomIdentifier() + randomFileExtension();
            final ShardId shardId = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 3));
            searchIndexInputs.put(
                new SearchIndexInput(
                    fileName,
                    sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, compoundFileName), allBytes.length),
                    randomIOContext(),
                    TestUtils.singleBlobContainer(compoundFileName, allBytes),
                    sharedBlobCacheService,
                    checksumAndLength.length,
                    offset
                ),
                checksumAndLength.checksum
            );
            offset += checksumAndLength.length;
        }

        return searchIndexInputs;
    }

    record ChecksumAndLength(String checksum, int length) {}

    static byte[] concatBytes(byte[] bytes1, byte[] bytes2) {
        byte[] newBytes = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, newBytes, 0, bytes1.length);
        System.arraycopy(bytes2, 0, newBytes, bytes1.length, bytes2.length);
        return newBytes;
    }

    private static Settings sharedCacheSettings(ByteSizeValue cacheSize, ByteSizeValue regionSize) {
        return Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            // The default is for RangeSize and RegionSize to be the same
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize)
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), randomBoolean())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .build();
    }
}
