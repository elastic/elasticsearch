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

package co.elastic.elasticsearch.stateless.engine;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.index.engine.LiveVersionMap;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.enforceSafeAccess;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.get;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isUnsafe;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.maybePutIndex;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.newDeleteVersionValue;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.newIndexVersionValue;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.newLiveVersionMap;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.pruneTombstones;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.putDelete;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.putIndex;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.randomIndexVersionValue;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.reclaimableRefreshRamBytes;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.refreshingBytes;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.versionLookupSize;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.IsEqual.equalTo;

public class StatelessLiveVersionMapTests extends ESTestCase {

    private static final long EMPTY_ARCHIVE_SIZE = RamUsageTester.ramUsed(new StatelessLiveVersionMapArchive(() -> 0L));
    private static final long EMPTY_LVM_SIZE = RamUsageTester.ramUsed(newLiveVersionMap(new StatelessLiveVersionMapArchive(() -> 0L)));

    public void testBasics() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        var id = "test";
        Translog.Location loc = randomTranslogLocation();
        var indexVersionValue = newIndexVersionValue(loc, 1, 1, 1);
        putIndex(map, id, indexVersionValue);
        assertEquals(indexVersionValue, get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
        refresh(map);
        assertThat(archive.archivePerGeneration().size(), equalTo(1));
        var archiveMap = archive.archivePerGeneration().get(preCommitGeneration.get() + 1);
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        assertEquals(indexVersionValue, get(map, id));
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        assertNull(get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));

        var deleteVersionValue = newDeleteVersionValue(1, 1, 1, 1);
        putDelete(map, id, deleteVersionValue);
        assertEquals(deleteVersionValue, get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
        refresh(map);
        assertEquals(deleteVersionValue, get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(1));
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(preCommitGeneration.get());
        assertEquals(deleteVersionValue, get(map, id));
        pruneTombstones(map, 2, 1);
        assertNull(get(map, id));

        // - a delete moves id to tombstone
        // - an index with the same id puts the id in the current map
        // - a refresh moves the id to the archive
        // In this case, we should get the new value from the archive.
        indexVersionValue = newIndexVersionValue(loc, 1, 1, 1);
        putIndex(map, id, indexVersionValue);
        assertEquals(indexVersionValue, get(map, id));
        deleteVersionValue = newDeleteVersionValue(2, 2, 2, 2);
        putDelete(map, id, deleteVersionValue);
        assertEquals(deleteVersionValue, get(map, id));
        var indexValueVersion2 = newIndexVersionValue(randomTranslogLocation(), 3, 3, 1);
        putIndex(map, id, indexValueVersion2);
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
        refresh(map);
        assertEquals(indexValueVersion2, get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(1));
        archiveMap = archive.archivePerGeneration().get(preCommitGeneration.get() + 1);
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(preCommitGeneration.get());
        assertNull(get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
    }

    public void testMultipleRefreshesWithFlush() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        var id = "test";
        putIndex(map, id, newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
        refresh(map);
        assertThat(archive.archivePerGeneration().size(), equalTo(1));
        var archiveMap = archive.archivePerGeneration().get(preCommitGeneration.get() + 1);
        assertNotNull(archiveMap);
        var update = newIndexVersionValue(randomTranslogLocation(), 2, 2, 1);
        putIndex(map, id, update);
        refresh(map);
        assertEquals(update, get(map, id));
        flush(map, currentGeneration, preCommitGeneration);
        // while the commit if propagating to the unpromotables, a new update is added
        var update2 = newIndexVersionValue(randomTranslogLocation(), 1, 1, 1);
        putIndex(map, id, update2);
        assertEquals(update2, get(map, id));
        refresh(map);
        // We should have two generations in the archive, since there were two refreshes before and after
        // the generation changed.
        assertThat(archive.archivePerGeneration().size(), equalTo(2));
        archiveMap = archive.archivePerGeneration().get(preCommitGeneration.get());
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        archiveMap = archive.archivePerGeneration().get(preCommitGeneration.get() + 1);
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        assertEquals(update2, get(map, id));
        archive.afterUnpromotablesRefreshed(preCommitGeneration.incrementAndGet());
        assertNull(get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
    }

    /**
     * In the case where a pruneTombstone happens between a refresh and afterUnpromotablesRefreshed,
     * we could return stale value from archive rather than delete from tombstone.
     */
    public void testPruneTombstonesBetweenRefreshAndUnpromotableRefresh() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        var id = "test";
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
        putIndex(map, id, newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        refresh(map);
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
        var deleteVersionValue = newDeleteVersionValue(2, 2, 2, 2);
        putDelete(map, id, deleteVersionValue);
        assertEquals(deleteVersionValue, get(map, id));
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
        refresh(map);
        assertThat(archive.getMinDeleteTimestamp(), equalTo(2L));
        pruneTombstones(map, 3, 3);
        assertEquals(deleteVersionValue, get(map, id));
        // A flush/commit happens
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(preCommitGeneration.get());
        assertEquals(deleteVersionValue, get(map, id));
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
        pruneTombstones(map, 3, 3);
        assertNull(get(map, id));
    }

    public void testArchiveMinDeleteTimestamp() {
        // current segment generation is 0
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        assertEquals(archive.getMinDeleteTimestamp(), Long.MAX_VALUE);
        // the timestamp is not changed throughout refreshes that don't see deletes
        putIndex(map, "1", newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        refresh(map);
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));

        putDelete(map, "1", newDeleteVersionValue(2, 2, 2, 2));
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
        refresh(map);
        assertThat(archive.getMinDeleteTimestamp(), equalTo(2L));
        // A refresh bringing in an earlier delete timestamp (is it possible?) would update that generation's
        // recorded min delete timestamp.
        putDelete(map, "2", newDeleteVersionValue(2, 2, 2, 1));
        refresh(map);
        assertThat(archive.getMinDeleteTimestamp(), equalTo(1L));
        // New segment generation (flush on index shard)
        preCommitGeneration.incrementAndGet();
        currentGeneration.incrementAndGet();
        putIndex(map, "3", newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        putDelete(map, "3", newDeleteVersionValue(1, 1, 1, 3));
        refresh(map);
        assertThat(archive.getMinDeleteTimestamp(), equalTo(1L));
        // The commit reaches the search shards
        archive.afterUnpromotablesRefreshed(preCommitGeneration.get());
        assertThat(archive.getMinDeleteTimestamp(), equalTo(3L));
        // New flush, and it gets to the search shards...
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(preCommitGeneration.get());
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
    }

    public void testUnsafeMapIsRecordedInArchiveUntilUnpromotablesAreRefreshed() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        assertFalse(isUnsafe(map));
        maybePutIndex(map, "1", newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        assertTrue(isUnsafe(map));
        // A refresh moves the unsafe map from current to old and then to archive.
        // Do at least one refresh
        var localRefreshes = randomIntBetween(1, 5);
        for (int i = 0; i < localRefreshes; i++) {
            // Flushes don't change anything as long as they haven't reached the search shards
            if (randomBoolean()) {
                // flush
                preCommitGeneration.incrementAndGet();
                currentGeneration.incrementAndGet();
            }
            refresh(map);
        }
        // We should still remember that the archive received an unsafe map
        assertTrue(isUnsafe(map));
        // New flush, and it gets to the search shards...
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(preCommitGeneration.get());
        assertFalse(isUnsafe(map));
    }

    public void testUnsafeMapClearedByUnpromotableRefresh() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong();
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        assertFalse(isUnsafe(map));
        maybePutIndex(map, "1", newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        assertTrue(isUnsafe(map));
        // A commit happens (e.g. due to a switch from unsafe to safe when a get to an unsafe map is received)
        flush(map, currentGeneration, preCommitGeneration);
        assertThat(archive.getMinSafeGeneration(), Matchers.equalTo(preCommitGeneration.get() + 1));
        // We should still remember that the archive received an unsafe map
        assertTrue(isUnsafe(map));
        // It gets to the search shards...
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        // Since we conservatively wait for one more generation to clear the isUnsafe flag, in this case
        // we would need to wait for an extra commit to clear the flag, although it is not necessary.
        assertTrue(isUnsafe(map));
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        assertFalse(isUnsafe(map));
    }

    public void testUnsafeMapNotClearedDueToLocalRefresh() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong();
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        maybePutIndex(map, "1", newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        // commit, hold on to unprmotable responses
        assertThat(archive.getMinSafeGeneration(), equalTo(-1L));
        flush(map, currentGeneration, preCommitGeneration);
        var minSafeGeneration = archive.getMinSafeGeneration();
        assertThat(minSafeGeneration, Matchers.equalTo(preCommitGeneration.get() + 1));
        assertTrue(isUnsafe(map));
        // while unpromotable refresh is happening, we index mode
        maybePutIndex(map, "2", newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        refresh(map);
        assertThat(archive.getMinSafeGeneration(), Matchers.equalTo(preCommitGeneration.get() + 1));
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        // Shouldn't be cleared since the commit that is inflight to search shards only had id 1
        assertTrue(isUnsafe(map));
        flush(map, currentGeneration, preCommitGeneration);
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        assertFalse(isUnsafe(map));
    }

    /**
     * Due to possibility of parallel indexing and local refreshes during a flush, we'd need to conservatively set minSafeGeneration
     * in the archive to preCommitGeneration + 1, as otherwise we'd risk not seeing some documents that are indexed while a flush is
     * happening and the live version map is unsafe.
     */
    public void testUnsafeMapDataIndexedDuringFlush() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong();
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        maybePutIndex(map, "1", newIndexVersionValue(randomTranslogLocation(), 1, 0, 1));
        assertTrue(isUnsafe(map));
        // Flush starts
        preCommitGeneration.set(currentGeneration.get() + 1);
        // Index data during the flush, after we have committed the index writer, but still unsafe
        assertTrue(isUnsafe(map));
        int count = randomIntBetween(1, 3);
        for (int i = 0; i < count; i++) {
            maybePutIndex(map, randomIdentifier(), newIndexVersionValue(randomTranslogLocation(), 1, i + 1, 1));
            if (randomBoolean()) {
                refresh(map);
            }
        }
        refresh(map); // A refresh (version_table_flush) that happens after the index writer is committed. See {@link InternalEngine#flush}
        assertThat(archive.getMinSafeGeneration(), Matchers.equalTo(preCommitGeneration.get() + 1));
        currentGeneration.incrementAndGet();
        // Flush ends
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        // should still be unsafe
        assertTrue(isUnsafe(map));
        currentGeneration.incrementAndGet();
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        assertFalse(isUnsafe(map));
    }

    public void testArchiveMemoryUsed() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        // Index and refresh on an unsafe map
        assertEquals(0, archive.getRamBytesUsed());
        assertEquals(0, archive.getReclaimableRamBytes());
        assertEquals(0, archive.getRefreshingRamBytes());
        maybePutIndex(map, "1", randomIndexVersionValue());
        assertEquals(0, archive.getRamBytesUsed());
        assertTrue(isUnsafe(map));
        refresh(map);
        assertEquals(0, archive.getRamBytesUsed());
        // Index and refresh on an safe map
        enforceSafeAccess(map);
        maybePutIndex(map, "2", randomIndexVersionValue());
        assertEquals(0, archive.getRamBytesUsed());
        assertEquals(0, archive.getReclaimableRamBytes());
        refresh(map);
        assertThat(archive.getReclaimableRamBytes(), greaterThan(0L));
        assertEquals(0, archive.getRefreshingRamBytes());
        assertArchiveMemoryBytesUsedIsCorrect(archive);
        flush(map, currentGeneration, preCommitGeneration);
        assertArchiveMemoryBytesUsedIsCorrect(archive);
        assertEquals(0, archive.getReclaimableRamBytes());
        assertThat(archive.getRefreshingRamBytes(), greaterThan(0L));
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        assertEquals(0, archive.getRamBytesUsed());
        assertEquals(0, archive.getReclaimableRamBytes());
        assertEquals(0, archive.getRefreshingRamBytes());
        // Randomly test indexing (assuming safe map), refresh, flush and unpromotable refresh
        IntStream.range(1, randomIntBetween(10, 20)).forEach(i -> {
            var sizeBeforeIndexing = archive.getRamBytesUsed();
            var noOfOps = randomIntBetween(0, 10);
            IntStream.range(0, noOfOps).forEach(opCounter -> putIndex(map, randomIdentifier(), randomIndexVersionValue()));
            assertEquals(sizeBeforeIndexing, archive.getRamBytesUsed());
            var noOfRefreshes = randomIntBetween(0, 3);
            IntStream.range(0, noOfRefreshes).forEach(refreshCounter -> refresh(map));
            assertArchiveMemoryBytesUsedIsCorrect(archive);
            if (noOfOps > 0) {
                assertThat(reclaimableRefreshRamBytes(map), greaterThan(0L));
            }
            List<Long> flushedGenerations = new ArrayList<>();
            var noOfFlushes = randomIntBetween(0, 3);
            IntStream.range(0, noOfFlushes).forEach(flushCounter -> {
                flush(map, currentGeneration, preCommitGeneration);
                assertArchiveMemoryBytesUsedIsCorrect(archive);
                flushedGenerations.add(currentGeneration.get());
            });
            if (noOfRefreshes > 0 && noOfFlushes > 0) {
                assertEquals(0, archive.getReclaimableRamBytes());
                if (noOfOps > 0) {
                    assertThat(refreshingBytes(map), greaterThan(0L));
                }
            }
            if (randomBoolean()) {
                // Unpromotable refreshes might come back in a different order
                Collections.shuffle(flushedGenerations, random());
            }
            for (var generation : flushedGenerations) {
                archive.afterUnpromotablesRefreshed(generation);
                assertArchiveMemoryBytesUsedIsCorrect(archive);
            }
        });
        assertArchiveMemoryBytesUsedIsCorrect(archive);
        assertEquals(0, refreshingBytes(map));
    }

    public void testLiveVersionMapMemoryUsed() {
        AtomicLong currentGeneration = new AtomicLong(0);
        AtomicLong preCommitGeneration = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(preCommitGeneration::get);
        var map = newLiveVersionMap(archive);
        long emptyMapActualMemUsed = RamUsageTester.ramUsed(map);
        IntStream.range(10000, randomIntBetween(10001, 100000))
            .forEach(i -> { putIndex(map, randomIdentifier(), randomIndexVersionValue()); });
        var preRefreshActualMemUsed = RamUsageTester.ramUsed(map);
        var preRefreshCalculatedMemUsed = map.ramBytesUsed();
        // The thresholds used here are based on the ones used in {@link LiveVersionMapTests#testRamBytesUsed()}
        // as {@link org.elasticsearch.index.engine.LiveVersionMap.ramBytesUsed} is an under-estimation compared to the
        // actual memory usage.
        assertEquals(preRefreshCalculatedMemUsed, preRefreshActualMemUsed, preRefreshActualMemUsed / 2);
        assertLVMBytesUsedIsCorrect(map);
        refresh(map);
        var postRefreshActualMemUsed = RamUsageTester.ramUsed(map);
        var postRefreshCalculatedMemUsed = map.ramBytesUsed();
        // There is a small variation since we move entries to the archive which is not exactly the same data structure as the LVM map.
        assertEquals(postRefreshActualMemUsed, preRefreshActualMemUsed, preRefreshActualMemUsed / 10);
        assertEquals(preRefreshCalculatedMemUsed, postRefreshCalculatedMemUsed, preRefreshCalculatedMemUsed / 10);
        assertLVMBytesUsedIsCorrect(map);
        flush(map, currentGeneration, preCommitGeneration);
        assertEquals(RamUsageTester.ramUsed(map), postRefreshActualMemUsed);
        assertLVMBytesUsedIsCorrect(map);
        archive.afterUnpromotablesRefreshed(currentGeneration.get());
        assertEquals(RamUsageTester.ramUsed(map), emptyMapActualMemUsed);
        assertLVMBytesUsedIsCorrect(map);
    }

    private static void assertLVMBytesUsedIsCorrect(LiveVersionMap liveVersionMap) {
        var actualMemoryUsed = RamUsageTester.ramUsed(liveVersionMap);
        assertEquals(EMPTY_LVM_SIZE + liveVersionMap.ramBytesUsed(), actualMemoryUsed, actualMemoryUsed / 2);
    }

    private static void assertArchiveMemoryBytesUsedIsCorrect(StatelessLiveVersionMapArchive archive) {
        var actualMemoryUsed = RamUsageTester.ramUsed(archive);
        assertEquals(EMPTY_ARCHIVE_SIZE + archive.getRamBytesUsed(), actualMemoryUsed, actualMemoryUsed / 2);
    }

    private void flush(LiveVersionMap map, AtomicLong currentGeneration, AtomicLong preCommitGeneration) {
        preCommitGeneration.set(currentGeneration.get() + 1);
        refresh(map); // A refresh (version_table_flush) that happens after the index writer is committed. See {@link InternalEngine#flush}
        currentGeneration.incrementAndGet();
    }

    private void refresh(LiveVersionMap map) {
        try {
            map.beforeRefresh();
            map.afterRefresh(randomBoolean());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private Translog.Location randomTranslogLocation() {
        return randomBoolean() ? null : new Translog.Location(randomNonNegativeLong(), randomNonNegativeLong(), randomInt());
    }
}
