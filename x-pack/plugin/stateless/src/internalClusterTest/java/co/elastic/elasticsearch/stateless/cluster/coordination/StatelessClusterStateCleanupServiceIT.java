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

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryPlugin;
import co.elastic.elasticsearch.stateless.StatelessMockRepositoryStrategy;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class StatelessClusterStateCleanupServiceIT extends AbstractStatelessIntegTestCase {
    private static final Logger logger = LogManager.getLogger(StatelessClusterStateCleanupServiceIT.class);
    private final long NUM_NODES = 3;
    private MockLogAppender mockLogAppender;
    private ClusterStateBlockingStrategy strategy = new ClusterStateBlockingStrategy();

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockLogAppender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger(StatelessClusterStateCleanupService.class), mockLogAppender);
        mockLogAppender.start();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        Loggers.removeAppender(LogManager.getLogger(StatelessClusterStateCleanupService.class), mockLogAppender);
        mockLogAppender.stop();
        super.tearDown();
    }

    private void resetMockLogAppender() {
        Loggers.removeAppender(LogManager.getLogger(StatelessClusterStateCleanupService.class), mockLogAppender);
        mockLogAppender.stop();
        mockLogAppender = new MockLogAppender();
        Loggers.addAppender(LogManager.getLogger(StatelessClusterStateCleanupService.class), mockLogAppender);
        mockLogAppender.start();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), StatelessMockRepositoryPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Minimize time to master failover.
            .put(HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
            // Need to set the ObjectStoreType to MOCK for the StatelessMockRepository plugin.
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK.toString().toLowerCase(Locale.ROOT));
    }

    public void restartMasterNode(StatelessMockRepositoryStrategy repositoryStrategy) throws Exception {
        String nodeName = internalCluster().getMasterName();
        internalCluster().restartNode(nodeName);
        setNodeRepositoryStrategy(nodeName, repositoryStrategy);
        ensureGreen();
    }

    /**
     * Returns a list of term directory names (1/, 2/, etc.) that are present in the blob store.
     */
    public static List<String> getTermDirectories() throws IOException {
        var objectStoreService = internalCluster().getInstance(ObjectStoreService.class, internalCluster().getRandomNodeName());
        var blobContainerForClusterState = objectStoreService.getClusterStateBlobContainer();
        var clusterStateDirectories = blobContainerForClusterState.children(OperationPurpose.CLUSTER_STATE);
        return clusterStateDirectories.keySet()
            .stream()
            // The blob container has subdirectories for each term as well as the heartbeats. Exclude the heartbeat/ directory
            // and all that should remain is term (1/, 2/, etc.) directories.
            .filter(key -> {
                if (key.equals(StatelessHeartbeatStore.HEARTBEAT_BLOB) == false) {
                    assertThat("Expected a number as directory name: " + key, key.chars().allMatch(Character::isDigit), is(true));
                    return true;
                }
                return false;
            })
            .collect(Collectors.toList());
    }

    /**
     * Tests that a delayed task, scheduled by the master node after election, will clean up cluster state for all older.
     * terms present. Starts up a cluster of master-only nodes and restarts the master a few times to iterate through a
     * few terms. Then checks that the term state is present before adjusting the delay period to run quickly, and does
     * one final master fail-over to prompt a quick cleanup.
     */
    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterStateCleanupService:DEBUG",
        reason = "The logic being tested logs at DEBUG level, so this will aid debugging."
    )
    public void testClusterStateCleanup() throws Exception {
        // Start some master nodes. The nodes will have a default delay of 1-minute for cluster state cleanup.
        logger.info("---> Starting up " + NUM_NODES + " master-only nodes.");
        for (int i = 0; i < NUM_NODES; i++) {
            startMasterOnlyNode(strategy);
        }
        // Wait for an election.
        ensureGreen();

        logger.info("---> Failing over the master node multiple times to generate new cluster state term directories in the blob store.");
        for (int i = 0; i < NUM_NODES; ++i) {
            logger.info("---> Iteration: " + (i + 1));
            restartMasterNode(strategy);
        }

        long numTerms = getTermDirectories().size();
        assertThat(
            Strings.format("Expected to find at least [%d] term directories but found [%d]", NUM_NODES, numTerms),
            numTerms,
            is(greaterThan(NUM_NODES))
        );
        logger.info("---> Found '" + numTerms + " ' term directories after causing master failovers by restarting master nodes");

        // Now we want cluster cleanup to happen more quickly in order to test it.
        logger.info("---> Setting a 1-second delay before cleaning up old cluster state. Will go into effect for future scheduling.");
        updateClusterSettings(
            Settings.builder()
                .put(StatelessClusterStateCleanupService.CLUSTER_STATE_CLEANUP_DELAY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
        );

        logger.info("---> Restarting the master node, to trigger a new election with a short delay for the cleanup task.");
        restartMasterNode(strategy);

        logger.info("---> A new master should have been elected, waiting for cluster state cleanup to run.");
        assertBusy(() -> {
            List<String> terms = getTermDirectories();
            assertThat("Expected a single term directory: " + terms.size(), terms.size(), is(equalTo(1)));

            long currentTerm = clusterAdmin().prepareState().get().getState().term();
            assertThat(
                Strings.format("Expected the latest term directory [%s] to match the latest term [%d]", terms.get(0), currentTerm),
                Long.parseLong(terms.get(0)),
                is(equalTo(currentTerm))
            );
        });

        logger.info("---> Starting a full cluster restart to ensure cluster state is still readable.");
        internalCluster().fullRestart();
        ensureGreen();
    }

    /**
     * Tests that the cluster state cleanup task retries on errors.
     */
    @TestLogging(
        value = "co.elastic.elasticsearch.stateless.cluster.coordination.StatelessClusterStateCleanupService:DEBUG",
        reason = "Testing that transient errors are logged at DEBUG level."
    )
    public void testClusterStateCleanupRetryability() throws Exception {
        logger.info("---> Starting up " + NUM_NODES + " master-only nodes.");
        for (int i = 0; i < NUM_NODES; i++) {
            startMasterOnlyNode(strategy);
        }
        // Wait for an election.
        ensureGreen();

        logger.info("---> Failing over the master node multiple times to generate new cluster state term directories in the blob store.");
        for (int i = 0; i < NUM_NODES; ++i) {
            logger.info("---> Iteration: " + (i + 1));
            restartMasterNode(strategy);
        }

        long numTerms = getTermDirectories().size();
        assertThat(
            Strings.format("Expected to find at least [%d] term directories but found [%d]", NUM_NODES, numTerms),
            numTerms,
            is(greaterThan(NUM_NODES))
        );
        logger.info("---> Found '" + numTerms + " ' term directories after causing master failovers by restarting master nodes");

        // Now we want cluster cleanup to happen more quickly in order to test it.
        logger.info("---> Setting a 1-second delay before cleaning up old cluster state. Will go into effect for future scheduling.");
        updateClusterSettings(
            Settings.builder()
                .put(StatelessClusterStateCleanupService.CLUSTER_STATE_CLEANUP_DELAY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
        );

        logger.info("---> Setting the mock repository to throw a retryable error on cleanup operation.");
        var nodeNames = internalCluster().getNodeNames();
        for (var nodeName : nodeNames) {
            var strategy = (ClusterStateBlockingStrategy) getNodeRepositoryStrategy(nodeName);
            strategy.throwOneIOExceptionOnClusterStateTermCleanup();
        }

        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "StatelessClusterStateCleanupService, expect transient error to produce a DEBUG log message",
                StatelessClusterStateCleanupService.class.getName(),
                Level.DEBUG,
                "*retrying action that failed*"
            )
        );

        logger.info("---> Stopping the master node, to trigger another master election.");
        internalCluster().stopCurrentMasterNode();
        ensureGreen();

        assertBusy(mockLogAppender::assertAllExpectationsMatched);

        logger.info("---> Check that the cleanup task succeeds.");
        assertBusy(() -> {
            List<String> terms = getTermDirectories();
            assertThat("Expected a single term directory: " + terms.size(), terms.size(), is(equalTo(1)));

            long currentTerm = clusterAdmin().prepareState().get().getState().term();
            assertThat(
                Strings.format("Expected the latest term directory [%s] to match the latest term [%d]", terms.get(0), currentTerm),
                Long.parseLong(terms.get(0)),
                is(equalTo(currentTerm))
            );
        });
    }

    /**
     * A mock repository strategy implementation that manipulates cluster state {@link BlobContainer#delete(OperationPurpose)} operations
     * run against the blob store.
     */
    public static class ClusterStateBlockingStrategy extends StatelessMockRepositoryStrategy {
        private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(
            ClusterStateBlockingStrategy.class
        );

        // This flag indicates whether an operation should block.
        private final AtomicBoolean clusterStateTermCleanupShouldBlock = new AtomicBoolean(false);

        // This latch is set to 1 when operations should be blocked, and decremented to 0 when they should resume.
        // Operations will wait on the latch until countDown reaches zero and then resume.
        private volatile CountDownLatch clusterStateTermCleanupShouldBlockedLatch = new CountDownLatch(0);

        // Injects throwing an error contacting the blob store.
        private final AtomicBoolean clusterStateTermCleanupThrowsIOException = new AtomicBoolean(false);

        /**
         * Optionally block cluster state deletion by term.
         */
        @Override
        public DeleteResult blobContainerDelete(CheckedSupplier<DeleteResult, IOException> originalSupplier, OperationPurpose purpose)
            throws IOException {
            assert purpose.equals(OperationPurpose.CLUSTER_STATE);
            maybeBlockClusterStateTermCleanup();
            maybeThrowClusterStateTermCleanup();
            return originalSupplier.get();
        }

        /**
         * Blocks if {@link #blockClusterStateTermCleanup} has been called.
         * Any blocked callers will not be released until {@link #unblockClusterStateTermCleanup} is called.
         */
        private void maybeBlockClusterStateTermCleanup() {
            if (clusterStateTermCleanupShouldBlock.get() == false) {
                return;
            }

            safeAwait(clusterStateTermCleanupShouldBlockedLatch);
        }

        /**
         * Throws one Exception if {@link #throwOneIOExceptionOnClusterStateTermCleanup} has been called.
         */
        private void maybeThrowClusterStateTermCleanup() throws IOException {
            if (clusterStateTermCleanupThrowsIOException.compareAndExchange(true, false)) {
                logger.info("Artificial IOException for cluster state cleanup.");
                throw new IOException("Artificial IOException");
            }
        }

        public void blockClusterStateTermCleanup() {
            if (clusterStateTermCleanupShouldBlock.compareAndExchange(false, true)) {
                clusterStateTermCleanupShouldBlockedLatch = new CountDownLatch(1);
            }
        }

        public void unblockClusterStateTermCleanup() {
            if (clusterStateTermCleanupShouldBlock.compareAndExchange(true, false)) {
                clusterStateTermCleanupShouldBlockedLatch.countDown();
            }
        }

        /**
         * Sets a flag to throw one IOException on the next operation.
         */
        public void throwOneIOExceptionOnClusterStateTermCleanup() {
            clusterStateTermCleanupThrowsIOException.set(true);
        }
    }
}
