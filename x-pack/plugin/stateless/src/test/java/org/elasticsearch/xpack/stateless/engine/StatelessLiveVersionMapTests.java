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

import org.elasticsearch.index.engine.LiveVersionMap;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.get;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.isUnsafe;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.maybePutIndex;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.newDeleteVersionValue;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.newIndexVersionValue;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.newLiveVersionMap;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.pruneTombstones;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.putDelete;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.putIndex;
import static org.elasticsearch.index.engine.LiveVersionMapTestUtils.versionLookupSize;
import static org.hamcrest.core.IsEqual.equalTo;

public class StatelessLiveVersionMapTests extends ESTestCase {

    public void testBasics() throws Exception {
        AtomicLong generation = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(generation::get);
        var map = newLiveVersionMap(archive);
        var id = "test";
        Translog.Location loc = randomTranslogLocation();
        var indexVersionValue = newIndexVersionValue(loc, 1, 1, 1);
        putIndex(map, id, indexVersionValue);
        assertEquals(indexVersionValue, get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
        refresh(map);
        assertThat(archive.archivePerGeneration().size(), equalTo(1));
        var archiveMap = archive.archivePerGeneration().get(generation.get() + 1);
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        assertEquals(indexVersionValue, get(map, id));
        // A flush/commit happens
        archive.afterUnpromotablesRefreshed(generation.incrementAndGet());
        assertNull(get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));

        var deleteVersionValue = newDeleteVersionValue(1, 1, 1, 1);
        putDelete(map, id, deleteVersionValue);
        assertEquals(deleteVersionValue, get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
        refresh(map);
        assertEquals(deleteVersionValue, get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(1));
        // A flush/commit happens
        archive.afterUnpromotablesRefreshed(generation.incrementAndGet());
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
        archiveMap = archive.archivePerGeneration().get(generation.get() + 1);
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        // A flush/commit happens
        archive.afterUnpromotablesRefreshed(generation.incrementAndGet());
        assertNull(get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
    }

    public void testMultipleRefreshesWithFlush() throws Exception {
        AtomicLong generation = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(generation::get);
        var map = newLiveVersionMap(archive);
        var id = "test";
        putIndex(map, id, newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
        refresh(map);
        assertThat(archive.archivePerGeneration().size(), equalTo(1));
        var archiveMap = archive.archivePerGeneration().get(generation.get() + 1);
        assertNotNull(archiveMap);
        var update = newIndexVersionValue(randomTranslogLocation(), 2, 2, 1);
        putIndex(map, id, update);
        refresh(map);
        assertEquals(update, get(map, id));
        // A flush/commit happens
        generation.incrementAndGet();
        // while the commit if propagating to the unpromotables, a new update is added
        var update2 = newIndexVersionValue(randomTranslogLocation(), 1, 1, 1);
        putIndex(map, id, update2);
        assertEquals(update2, get(map, id));
        refresh(map);
        // We should have two generations in the archive, since there were two refreshes before and after
        // the generation changed.
        assertThat(archive.archivePerGeneration().size(), equalTo(2));
        archiveMap = archive.archivePerGeneration().get(generation.get());
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        archiveMap = archive.archivePerGeneration().get(generation.get() + 1);
        assertThat(versionLookupSize(archiveMap), equalTo(1));
        assertEquals(update2, get(map, id));
        archive.afterUnpromotablesRefreshed(generation.incrementAndGet());
        assertNull(get(map, id));
        assertThat(archive.archivePerGeneration().size(), equalTo(0));
    }

    /**
     * In the case where a pruneTombstone happens between a refresh and afterUnpromotablesRefreshed,
     * we could return stale value from archive rather than delete from tombstone.
     */
    public void testPruneTombstonesBetweenRefreshAndUnpromotableRefresh() throws Exception {
        AtomicLong generation = new AtomicLong(0);
        var archive = new StatelessLiveVersionMapArchive(generation::get);
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
        archive.afterUnpromotablesRefreshed(generation.incrementAndGet());
        assertEquals(deleteVersionValue, get(map, id));
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
        pruneTombstones(map, 3, 3);
        assertNull(get(map, id));
    }

    public void testArchiveMinDeleteTimestamp() throws IOException {
        AtomicLong generation = new AtomicLong(); // current segment generation is 0
        var archive = new StatelessLiveVersionMapArchive(generation::get);
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
        generation.incrementAndGet();
        putIndex(map, "3", newIndexVersionValue(randomTranslogLocation(), 1, 1, 1));
        putDelete(map, "3", newDeleteVersionValue(1, 1, 1, 3));
        refresh(map);
        assertThat(archive.getMinDeleteTimestamp(), equalTo(1L));
        // The commit reaches the search shards
        archive.afterUnpromotablesRefreshed(generation.get());
        assertThat(archive.getMinDeleteTimestamp(), equalTo(3L));
        // New flush, and it gets to the search shards...
        archive.afterUnpromotablesRefreshed(generation.incrementAndGet());
        assertThat(archive.getMinDeleteTimestamp(), equalTo(Long.MAX_VALUE));
    }

    public void testUnsafeMapIsRecordedInArchiveUntilUnpromotablesAreRefreshed() throws IOException {
        AtomicLong generation = new AtomicLong(); // current segment generation is 0
        var archive = new StatelessLiveVersionMapArchive(generation::get);
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
                generation.incrementAndGet();
            }
            refresh(map);
        }
        // We should still remember that the archive received an unsafe map
        assertTrue(isUnsafe(map));
        // New flush, and it gets to the search shards...
        archive.afterUnpromotablesRefreshed(generation.incrementAndGet());
        assertFalse(isUnsafe(map));
    }

    private void refresh(LiveVersionMap map) throws IOException {
        map.beforeRefresh();
        map.afterRefresh(randomBoolean());
    }

    private Translog.Location randomTranslogLocation() {
        return randomBoolean() ? null : new Translog.Location(randomNonNegativeLong(), randomNonNegativeLong(), randomInt());
    }
}
