/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BoundedPendingStoreTests extends ESTestCase {

    private static final String LOGGER_NAME = BoundedPendingStore.class.getCanonicalName();
    private static final Instant BASE = Instant.parse("2026-11-20T10:00:00Z");

    private static UsageRecord record(int n) {
        return new UsageRecord(
            "id-" + n,
            BASE.plusSeconds(n),
            BASE.plusSeconds(n),
            UsageTypes.EXTERNAL_SOURCES_CPU,
            1000L + n,
            300,
            Map.of(),
            null,
            new SourceIdentity("node", "deployment", "cluster", "aws", "us-east-1")
        );
    }

    private static List<UsageRecord> records(int from, int toExclusive) {
        List<UsageRecord> result = new ArrayList<>();
        for (int i = from; i < toExclusive; i++) {
            result.add(record(i));
        }
        return result;
    }

    private static List<String> ids(List<UsageRecord> records) {
        return records.stream().map(UsageRecord::id).toList();
    }

    public void testRejectsNonPositiveBound() {
        expectThrows(IllegalArgumentException.class, () -> new BoundedPendingStore(0));
    }

    public void testKeepsOrderOldestFirst() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(records(0, 3));
        store.add(records(3, 5));
        assertThat(ids(store.next(10)), contains("id-0", "id-1", "id-2", "id-3", "id-4"));
        assertThat(ids(store.next(2)), contains("id-0", "id-1"));
        assertThat(store.next(0), empty());
        assertThat(store.size(), equalTo(5));
    }

    public void testNextDoesNotRemove() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(records(0, 2));
        store.next(2);
        assertThat(store.size(), equalTo(2));
    }

    public void testDropsOldestBeyondTheBoundAndLogsEachLoss() {
        BoundedPendingStore store = new BoundedPendingStore(3);
        store.add(records(0, 3));
        MockLog.assertThatLogger(
            () -> store.add(records(3, 5)),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation(
                "first loss",
                LOGGER_NAME,
                Level.ERROR,
                "dropping usage record [id-0] of type [" + UsageTypes.EXTERNAL_SOURCES_CPU + "] with quantity [1000] ms: *"
            ),
            new MockLog.SeenEventExpectation(
                "second loss",
                LOGGER_NAME,
                Level.ERROR,
                "dropping usage record [id-1] of type [" + UsageTypes.EXTERNAL_SOURCES_CPU + "] with quantity [1001] ms: *"
            ),
            new MockLog.UnseenEventExpectation("no other loss", LOGGER_NAME, Level.ERROR, "dropping usage record [id-2]*")
        );
        assertThat(ids(store.next(10)), contains("id-2", "id-3", "id-4"));
        assertThat(store.size(), equalTo(3));
        assertThat(store.droppedCount(), equalTo(2L));
    }

    public void testBatchLargerThanTheBoundKeepsTheNewest() {
        BoundedPendingStore store = new BoundedPendingStore(2);
        MockLog.assertThatLogger(
            () -> store.add(records(0, 5)),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation("loss 0", LOGGER_NAME, Level.ERROR, "dropping usage record [id-0]*"),
            new MockLog.SeenEventExpectation("loss 1", LOGGER_NAME, Level.ERROR, "dropping usage record [id-1]*"),
            new MockLog.SeenEventExpectation("loss 2", LOGGER_NAME, Level.ERROR, "dropping usage record [id-2]*")
        );
        assertThat(ids(store.next(10)), contains("id-3", "id-4"));
        assertThat(store.droppedCount(), equalTo(3L));
    }

    public void testNothingIsDroppedAtTheBound() {
        BoundedPendingStore store = new BoundedPendingStore(3);
        MockLog.assertThatLogger(
            () -> store.add(records(0, 3)),
            BoundedPendingStore.class,
            new MockLog.UnseenEventExpectation("no loss", LOGGER_NAME, Level.ERROR, "*")
        );
        assertThat(store.size(), equalTo(3));
        assertThat(store.droppedCount(), equalTo(0L));
    }

    public void testSameIdKeepsOneCopyAndItsPosition() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(records(0, 3));
        store.add(List.of(record(1), record(3), record(3)));
        assertThat(ids(store.next(10)), contains("id-0", "id-1", "id-2", "id-3"));
        assertThat(store.size(), equalTo(4));
    }

    public void testSameIdWithDifferentContentKeepsTheFirstAndLogsTheLoss() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(List.of(record(1)));
        UsageRecord different = new UsageRecord(
            "id-1",
            BASE,
            BASE,
            UsageTypes.EXTERNAL_SOURCES_CPU,
            42,
            300,
            Map.of(),
            null,
            new SourceIdentity("node", "deployment", "cluster", "aws", "us-east-1")
        );
        MockLog.assertThatLogger(
            () -> store.add(List.of(different)),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation(
                "conflict",
                LOGGER_NAME,
                Level.ERROR,
                "dropping usage record [id-1] of type [" + UsageTypes.EXTERNAL_SOURCES_CPU + "] with quantity [42] ms: *"
            )
        );
        assertThat(store.next(10), contains(record(1)));
        assertThat(store.droppedCount(), equalTo(1L));
    }

    public void testSameIdWithSameContentIsNotALoss() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(List.of(record(1)));
        MockLog.assertThatLogger(
            () -> store.add(List.of(record(1))),
            BoundedPendingStore.class,
            new MockLog.UnseenEventExpectation("no loss", LOGGER_NAME, Level.ERROR, "*")
        );
        assertThat(store.droppedCount(), equalTo(0L));
    }

    public void testReAddingDoesNotRefreshAgeSoItIsDroppedFirst() {
        BoundedPendingStore store = new BoundedPendingStore(3);
        store.add(records(0, 3));
        store.add(List.of(record(0)));
        MockLog.assertThatLogger(
            () -> store.add(List.of(record(3))),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation("oldest lost", LOGGER_NAME, Level.ERROR, "dropping usage record [id-0]*")
        );
        assertThat(ids(store.next(10)), contains("id-1", "id-2", "id-3"));
    }

    public void testAcknowledgeRemovesWithoutLogging() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(records(0, 4));
        MockLog.assertThatLogger(
            () -> store.acknowledge(Set.of("id-1", "id-3", "unknown")),
            BoundedPendingStore.class,
            new MockLog.UnseenEventExpectation("no loss", LOGGER_NAME, Level.ERROR, "*")
        );
        assertThat(ids(store.next(10)), contains("id-0", "id-2"));
        assertThat(store.droppedCount(), equalTo(0L));
    }

    public void testDropRemovesLogsReasonAndCounts() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(records(0, 3));
        MockLog.assertThatLogger(
            () -> store.drop(Set.of("id-1", "unknown"), "rejected as invalid"),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation(
                "dropped",
                LOGGER_NAME,
                Level.ERROR,
                "dropping usage record [id-1] of type ["
                    + UsageTypes.EXTERNAL_SOURCES_CPU
                    + "] with quantity [1001] ms: rejected as invalid"
            ),
            new MockLog.UnseenEventExpectation("unknown id is not logged", LOGGER_NAME, Level.ERROR, "dropping usage record [unknown]*")
        );
        assertThat(ids(store.next(10)), contains("id-0", "id-2"));
        assertThat(store.droppedCount(), equalTo(1L));
    }

    public void testOldestUsageTimestamp() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        assertThat(store.oldestUsageTimestamp(), equalTo(Optional.empty()));
        store.add(List.of(record(5), record(2), record(7)));
        assertThat(store.oldestUsageTimestamp(), equalTo(Optional.of(BASE.plusSeconds(2))));
        store.acknowledge(Set.of("id-2"));
        assertThat(store.oldestUsageTimestamp(), equalTo(Optional.of(BASE.plusSeconds(5))));
    }

    /**
     * The reporter is single flight but the metrics read {@code size} concurrently. Every method is synchronized, so this guards
     * against that being removed: the bound must hold at every observation and no record may be lost without being counted.
     */
    public void testConcurrentAddAndRead() throws Exception {
        int bound = 50;
        BoundedPendingStore store = new BoundedPendingStore(bound);
        int writers = 4;
        int perWriter = 200;
        AtomicBoolean boundExceeded = new AtomicBoolean();
        MockLog.assertThatLogger(() -> startInParallel(writers + 1, t -> {
            if (t == writers) {
                for (int i = 0; i < perWriter; i++) {
                    if (store.size() > bound || store.next(bound * 2).size() > bound) {
                        boundExceeded.set(true);
                    }
                    store.oldestUsageTimestamp();
                }
            } else {
                for (int i = 0; i < perWriter; i++) {
                    store.add(List.of(record(t * perWriter + i)));
                }
            }
        }),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation("losses are logged", LOGGER_NAME, Level.ERROR, "dropping usage record [*")
        );

        assertFalse("the bound was exceeded", boundExceeded.get());
        assertThat(store.size(), equalTo(bound));
        assertThat(store.droppedCount(), equalTo((long) writers * perWriter - bound));
    }

    public void testBoundOfOne() {
        BoundedPendingStore store = new BoundedPendingStore(1);
        MockLog.assertThatLogger(
            () -> store.add(records(0, 2)),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation("loss", LOGGER_NAME, Level.ERROR, "dropping usage record [id-0]*")
        );
        assertThat(ids(store.next(10)), contains("id-1"));
    }

    /** A retry of a record that was already acknowledged is accepted again: the Usage API deduplicates by id, not the store. */
    public void testIdCanBeAddedAgainAfterAcknowledge() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(records(0, 2));
        store.acknowledge(Set.of("id-0"));
        store.add(List.of(record(0)));
        assertThat(ids(store.next(10)), contains("id-1", "id-0"));
    }

    public void testDropOfAlreadyRemovedIdIsNotLoggedNorCounted() {
        BoundedPendingStore store = new BoundedPendingStore(10);
        store.add(records(0, 1));
        store.acknowledge(Set.of("id-0"));
        MockLog.assertThatLogger(
            () -> store.drop(Set.of("id-0"), "rejected"),
            BoundedPendingStore.class,
            new MockLog.UnseenEventExpectation("no loss", LOGGER_NAME, Level.ERROR, "*")
        );
        assertThat(store.droppedCount(), equalTo(0L));
    }

    public void testFactoryBuildsTheBoundedStoreWithPendingMax() {
        Settings settings = Settings.builder().put(UsageReportingSettings.PENDING_MAX.getKey(), 2).build();
        PendingStore store = UsageComponents.newPendingStore(UsageReportingSettings.of(settings));
        assertThat(store, instanceOf(BoundedPendingStore.class));
        MockLog.assertThatLogger(
            () -> store.add(records(0, 3)),
            BoundedPendingStore.class,
            new MockLog.SeenEventExpectation("loss", LOGGER_NAME, Level.ERROR, "dropping usage record [id-0]*")
        );
        assertThat(store.size(), equalTo(2));
    }
}
