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

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.elasticsearch.index.seqno.RetentionLeaseActions.RETAIN_ALL;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

public class RetentionLeaseActionsTests extends ESSingleNodeTestCase {

    public void testAddAction() {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");

        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);
        client()
                .execute(
                        RetentionLeaseActions.Add.INSTANCE,
                        new RetentionLeaseActions.AddRequest(indexService.getShard(0).shardId(), id, retainingSequenceNumber, source))
                .actionGet();

        final IndicesStatsResponse stats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();
        assertNotNull(stats.getShards());
        assertThat(stats.getShards(), arrayWithSize(1));
        assertNotNull(stats.getShards()[0].getRetentionLeaseStats());
        assertThat(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(2));
        final RetentionLease retentionLease = stats.getShards()[0].getRetentionLeaseStats().retentionLeases().get(id);
        assertThat(retentionLease.id(), equalTo(id));
        assertThat(retentionLease.retainingSequenceNumber(), equalTo(retainingSequenceNumber == RETAIN_ALL ? 0L : retainingSequenceNumber));
        assertThat(retentionLease.source(), equalTo(source));

        assertTrue(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(stats.getShards()[0].getShardRouting())));
    }

    public void testAddAlreadyExists() {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");

        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);
        client()
                .execute(
                        RetentionLeaseActions.Add.INSTANCE,
                        new RetentionLeaseActions.AddRequest(indexService.getShard(0).shardId(), id, retainingSequenceNumber, source))
                .actionGet();

        final long nextRetainingSequenceNumber =
                retainingSequenceNumber == RETAIN_ALL && randomBoolean() ? RETAIN_ALL
                        : randomLongBetween(Math.max(retainingSequenceNumber, 0L), Long.MAX_VALUE);

        final RetentionLeaseAlreadyExistsException e = expectThrows(
                RetentionLeaseAlreadyExistsException.class,
                () -> client()
                        .execute(
                                RetentionLeaseActions.Add.INSTANCE,
                                new RetentionLeaseActions.AddRequest(
                                        indexService.getShard(0).shardId(),
                                        id,
                                        nextRetainingSequenceNumber,
                                        source))
                        .actionGet());
        assertThat(e, hasToString(containsString("retention lease with ID [" + id + "] already exists")));
    }

    public void testRenewAction() throws InterruptedException {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");

        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);

        /*
         * When we renew the lease, we want to ensure that the timestamp on the thread pool clock has advanced. To do this, we sample how
         * often the thread pool clock advances based on the following setting. After we add the initial lease we sample the relative time.
         * Immediately before the renewal of the lease, we sleep long enough to ensure that an estimated time interval has elapsed, and
         * sample the thread pool to ensure the clock has in fact advanced.
         */
        final TimeValue estimatedTimeInterval = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(getInstanceFromNode(Node.class).settings());

        client()
                .execute(
                        RetentionLeaseActions.Add.INSTANCE,
                        new RetentionLeaseActions.AddRequest(indexService.getShard(0).shardId(), id, retainingSequenceNumber, source))
                .actionGet();

        /*
         * Sample these after adding the retention lease so that advancement here guarantees we have advanced past the timestamp on the
         * lease.
         */
        final ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        final long timestampUpperBound = threadPool.absoluteTimeInMillis();
        final long start = System.nanoTime();

        final IndicesStatsResponse initialStats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();

        assertNotNull(initialStats.getShards());
        assertThat(initialStats.getShards(), arrayWithSize(1));
        assertNotNull(initialStats.getShards()[0].getRetentionLeaseStats());
        assertThat(initialStats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(2));
        assertTrue(initialStats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(initialStats.getShards()[0].getShardRouting())));
        final RetentionLease initialRetentionLease =
                initialStats.getShards()[0].getRetentionLeaseStats().retentionLeases().get(id);

        final long nextRetainingSequenceNumber =
                retainingSequenceNumber == RETAIN_ALL && randomBoolean() ? RETAIN_ALL
                        : randomLongBetween(Math.max(retainingSequenceNumber, 0L), Long.MAX_VALUE);

        /*
         * Wait until the thread pool clock advances. Note that this will fail on a system when the system clock goes backwards during
         * execution of the test. The specific circumstances under which this can fail is if the system clock goes backwards more than the
         * suite timeout. It seems there is nothing simple that we can do to avoid this?
         */
        do {
            final long end = System.nanoTime();
            if (end - start < estimatedTimeInterval.nanos()) {
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(estimatedTimeInterval.nanos() - (end - start)));
            }
        } while (threadPool.absoluteTimeInMillis() <= timestampUpperBound);

        client()
                .execute(
                        RetentionLeaseActions.Renew.INSTANCE,
                        new RetentionLeaseActions.RenewRequest(indexService.getShard(0).shardId(), id, nextRetainingSequenceNumber, source))
                .actionGet();

        final IndicesStatsResponse renewedStats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();

        assertNotNull(renewedStats.getShards());
        assertThat(renewedStats.getShards(), arrayWithSize(1));
        assertNotNull(renewedStats.getShards()[0].getRetentionLeaseStats());
        assertThat(renewedStats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(2));
        assertTrue(renewedStats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(initialStats.getShards()[0].getShardRouting())));
        final RetentionLease renewedRetentionLease =
                renewedStats.getShards()[0].getRetentionLeaseStats().retentionLeases().get(id);
        assertThat(renewedRetentionLease.id(), equalTo(id));
        assertThat(
                renewedRetentionLease.retainingSequenceNumber(),
                equalTo(nextRetainingSequenceNumber == RETAIN_ALL ? 0L : nextRetainingSequenceNumber));
        assertThat(renewedRetentionLease.timestamp(), greaterThan(initialRetentionLease.timestamp()));
        assertThat(renewedRetentionLease.source(), equalTo(source));
    }

    public void testRenewNotFound() {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");

        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);

        final RetentionLeaseNotFoundException e = expectThrows(
                RetentionLeaseNotFoundException.class,
                () -> client()
                        .execute(
                                RetentionLeaseActions.Renew.INSTANCE,
                                new RetentionLeaseActions.RenewRequest(
                                        indexService.getShard(0).shardId(),
                                        id,
                                        retainingSequenceNumber,
                                        source))
                        .actionGet());
        assertThat(e, hasToString(containsString("retention lease with ID [" + id + "] not found")));
    }

    public void testRemoveAction() {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");

        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);
        client()
                .execute(
                        RetentionLeaseActions.Add.INSTANCE,
                        new RetentionLeaseActions.AddRequest(indexService.getShard(0).shardId(), id, retainingSequenceNumber, source))
                .actionGet();

        client()
                .execute(
                        RetentionLeaseActions.Remove.INSTANCE,
                        new RetentionLeaseActions.RemoveRequest(indexService.getShard(0).shardId(), id))
                .actionGet();

        final IndicesStatsResponse stats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();
        assertNotNull(stats.getShards());
        assertThat(stats.getShards(), arrayWithSize(1));
        assertNotNull(stats.getShards()[0].getRetentionLeaseStats());
        assertThat(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(1));
        assertTrue(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(stats.getShards()[0].getShardRouting())));
    }

    public void testRemoveNotFound() {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");

        final String id = randomAlphaOfLength(8);

        final RetentionLeaseNotFoundException e = expectThrows(
                RetentionLeaseNotFoundException.class,
                () -> client()
                        .execute(
                                RetentionLeaseActions.Remove.INSTANCE,
                                new RetentionLeaseActions.RemoveRequest(indexService.getShard(0).shardId(), id))
                        .actionGet());
        assertThat(e, hasToString(containsString("retention lease with ID [" + id + "] not found")));
    }

    public void testAddUnderBlock() throws InterruptedException {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");
        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);
        runActionUnderBlockTest(
                indexService,
                (shardId, actionLatch) ->
                        client().execute(
                                RetentionLeaseActions.Add.INSTANCE,
                                new RetentionLeaseActions.AddRequest(shardId, id, retainingSequenceNumber, source),
                                new ActionListener<RetentionLeaseActions.Response>() {

                                    @Override
                                    public void onResponse(final RetentionLeaseActions.Response response) {
                                        actionLatch.countDown();
                                    }

                                    @Override
                                    public void onFailure(final Exception e) {
                                        fail(e.toString());
                                    }

                                }));

        final IndicesStatsResponse stats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();
        assertNotNull(stats.getShards());
        assertThat(stats.getShards(), arrayWithSize(1));
        assertNotNull(stats.getShards()[0].getRetentionLeaseStats());
        assertThat(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(2));
        assertTrue(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(stats.getShards()[0].getShardRouting())));
        final RetentionLease retentionLease = stats.getShards()[0].getRetentionLeaseStats().retentionLeases().get(id);
        assertThat(retentionLease.id(), equalTo(id));
        assertThat(retentionLease.retainingSequenceNumber(), equalTo(retainingSequenceNumber == RETAIN_ALL ? 0L : retainingSequenceNumber));
        assertThat(retentionLease.source(), equalTo(source));
    }

    public void testRenewUnderBlock() throws InterruptedException {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");
        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);

        /*
         * When we renew the lease, we want to ensure that the timestamp on the thread pool clock has advanced. To do this, we sample how
         * often the thread pool clock advances based on the following setting. After we add the initial lease we sample the relative time.
         * Immediately before the renewal of the lease, we sleep long enough to ensure that an estimated time interval has elapsed, and
         * sample the thread pool to ensure the clock has in fact advanced.
         */
        final TimeValue estimatedTimeInterval = ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.get(getInstanceFromNode(Node.class).settings());

        client()
                .execute(
                        RetentionLeaseActions.Add.INSTANCE,
                        new RetentionLeaseActions.AddRequest(indexService.getShard(0).shardId(), id, retainingSequenceNumber, source))
                .actionGet();

        /*
         * Sample these after adding the retention lease so that advancement here guarantees we have advanced past the timestamp on the
         * lease.
         */
        final ThreadPool threadPool = getInstanceFromNode(ThreadPool.class);
        final long timestampUpperBound = threadPool.absoluteTimeInMillis();
        final long start = System.nanoTime();

        final IndicesStatsResponse initialStats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();

        assertNotNull(initialStats.getShards());
        assertThat(initialStats.getShards(), arrayWithSize(1));
        assertNotNull(initialStats.getShards()[0].getRetentionLeaseStats());
        assertThat(initialStats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(2));
        assertTrue(initialStats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(initialStats.getShards()[0].getShardRouting())));
        final RetentionLease initialRetentionLease = initialStats.getShards()[0].getRetentionLeaseStats().retentionLeases().get(id);

        final long nextRetainingSequenceNumber =
                retainingSequenceNumber == RETAIN_ALL && randomBoolean() ? RETAIN_ALL
                        : randomLongBetween(Math.max(retainingSequenceNumber, 0L), Long.MAX_VALUE);

        /*
         * Wait until the thread pool clock advances. Note that this will fail on a system when the system clock goes backwards during
         * execution of the test. The specific circumstances under which this can fail is if the system clock goes backwards more than the
         * suite timeout. It seems there is nothing simple that we can do to avoid this?
         */
        do {
            final long end = System.nanoTime();
            if (end - start < estimatedTimeInterval.nanos()) {
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(estimatedTimeInterval.nanos() - (end - start)));
            }
        } while (threadPool.absoluteTimeInMillis() <= timestampUpperBound);

        runActionUnderBlockTest(
                indexService,
                (shardId, actionLatch) ->
                        client().execute(
                                RetentionLeaseActions.Renew.INSTANCE,
                                new RetentionLeaseActions.RenewRequest(shardId, id, nextRetainingSequenceNumber, source),
                                new ActionListener<RetentionLeaseActions.Response>() {

                                    @Override
                                    public void onResponse(final RetentionLeaseActions.Response response) {
                                        actionLatch.countDown();
                                    }

                                    @Override
                                    public void onFailure(final Exception e) {
                                        fail(e.toString());
                                    }

                                }));

        final IndicesStatsResponse renewedStats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();

        assertNotNull(renewedStats.getShards());
        assertThat(renewedStats.getShards(), arrayWithSize(1));
        assertNotNull(renewedStats.getShards()[0].getRetentionLeaseStats());
        assertThat(renewedStats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(2));
        assertTrue(renewedStats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(renewedStats.getShards()[0].getShardRouting())));
        final RetentionLease renewedRetentionLease = renewedStats.getShards()[0].getRetentionLeaseStats().retentionLeases().get(id);
        assertThat(renewedRetentionLease.id(), equalTo(id));
        assertThat(
                renewedRetentionLease.retainingSequenceNumber(),
                equalTo(nextRetainingSequenceNumber == RETAIN_ALL ? 0L : nextRetainingSequenceNumber));
        assertThat(renewedRetentionLease.timestamp(), greaterThan(initialRetentionLease.timestamp()));
        assertThat(renewedRetentionLease.source(), equalTo(source));
    }

    public void testRemoveUnderBlock() throws InterruptedException {
        final Settings settings = Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build();
        final IndexService indexService = createIndex("index", settings);
        ensureGreen("index");
        final String id = randomAlphaOfLength(8);
        final long retainingSequenceNumber = randomBoolean() ? RETAIN_ALL : randomNonNegativeLong();
        final String source = randomAlphaOfLength(8);

        client()
                .execute(
                        RetentionLeaseActions.Add.INSTANCE,
                        new RetentionLeaseActions.AddRequest(indexService.getShard(0).shardId(), id, retainingSequenceNumber, source))
                .actionGet();

        runActionUnderBlockTest(
                indexService,
                (shardId, actionLatch) ->
                        client().execute(
                                RetentionLeaseActions.Remove.INSTANCE,
                                new RetentionLeaseActions.RemoveRequest(shardId, id),
                                new ActionListener<RetentionLeaseActions.Response>() {

                                    @Override
                                    public void onResponse(final RetentionLeaseActions.Response response) {
                                        actionLatch.countDown();
                                    }

                                    @Override
                                    public void onFailure(final Exception e) {
                                        fail(e.toString());
                                    }

                                }));

        final IndicesStatsResponse stats = client()
                .execute(
                        IndicesStatsAction.INSTANCE,
                        new IndicesStatsRequest().indices("index"))
                .actionGet();
        assertNotNull(stats.getShards());
        assertThat(stats.getShards(), arrayWithSize(1));
        assertNotNull(stats.getShards()[0].getRetentionLeaseStats());
        assertThat(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().leases(), hasSize(1));
        assertTrue(stats.getShards()[0].getRetentionLeaseStats().retentionLeases().contains(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(stats.getShards()[0].getShardRouting())));
    }

    /*
     * Tests that use this method are ensuring that the asynchronous usage of the permits API when permit acquisition is blocked is
     * correctly handler. In these scenarios, we first acquire all permits. Then we submit a request to one of the retention lease actions
     * (via the consumer callback). That invocation will go asynchronous and be queued, since all permits are blocked. Then we release the
     * permit block and except that the callbacks occur correctly. These assertions happen after returning from this method.
     */
    private void runActionUnderBlockTest(
            final IndexService indexService,
            final BiConsumer<ShardId, CountDownLatch> consumer) throws InterruptedException {

        final CountDownLatch blockedLatch = new CountDownLatch(1);
        final CountDownLatch unblockLatch = new CountDownLatch(1);
        indexService.getShard(0).acquireAllPrimaryOperationsPermits(
                new ActionListener<Releasable>() {

                    @Override
                    public void onResponse(final Releasable releasable) {
                        try (Releasable ignore = releasable) {
                            blockedLatch.countDown();
                            unblockLatch.await();
                        } catch (final InterruptedException e) {
                            onFailure(e);
                        }
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        fail(e.toString());
                    }

                },
                TimeValue.timeValueHours(1));

        blockedLatch.await();

        final CountDownLatch actionLatch = new CountDownLatch(1);

        consumer.accept(indexService.getShard(0).shardId(), actionLatch);

        unblockLatch.countDown();
        actionLatch.await();
    }

}
