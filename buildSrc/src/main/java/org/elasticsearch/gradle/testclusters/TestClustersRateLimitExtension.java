package org.elasticsearch.gradle.testclusters;

import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This extensions was meant to be used internally by testclusters
 *
 * It holds synchronization primitives needed to implement the rate limiting.
 * This is tricky because we can't use Gradle workers as there's no way to make sure that tests and their clusters are
 * allocated atomically, so we could be in a situation where all workers are tests waiting for clusters to start up.
 *
 * Also auto configures cleanup of executors to make sure we don't leak threads in the daemon.
 */
public class TestClustersRateLimitExtension {

    private static final int EXECUTOR_SHUTDOWN_TIMEOUT = 1;
    private static final TimeUnit EXECUTOR_SHUTDOWN_TIMEOUT_UNIT = TimeUnit.MINUTES;

    private static final Logger logger =  Logging.getLogger(TestClustersRateLimitExtension.class);

    private final Semaphore globalSemaphore;
    private final ExecutorService executorService;
    private final TestClusterCleanupOnShutdown cleanupThread;

    public TestClustersRateLimitExtension(int maxPermits) {
        globalSemaphore = new Semaphore(maxPermits);
        executorService = Executors.newCachedThreadPool();
        cleanupThread = new TestClusterCleanupOnShutdown();
        executorService.submit(cleanupThread);
    }

    static void withPermits(Project project, int requiredPermits, Supplier<Collection<ElasticsearchCluster>> startedClusters) {
        if (requiredPermits > TestClustersRateLimitExtension.maxPermits(project)) {
            throw new TestClustersException("Clusters for " + project.getPath() + " are too large, requires " + requiredPermits +
                " nodes, but this system only supports a maximum of " + TestClustersRateLimitExtension.maxPermits(project) + " total"
            );
        }
        // It seems that Gradle runs `beforeActions` and `afterExecute` in the same single worker so
        // blocking here would mean that all of these hooks are blocked and the permits are never
        // released.
        // To prevent deadlocking, we have to use to make use if a thread that blocks when no permits are
        // available.
        // The task will eventually block when waiting for the cluster. We don't count that towards the
        // timeout.
        getSelfAsExtension(project).executorService.submit(() -> {
            Thread.currentThread().setName("TestClusters rate limit for " + project.getPath());
            logger.info(
                "Will acquire {} permits for {}",
                requiredPermits, project.getPath()
            );
            Instant startedAt = Instant.now();
            getSelfAsExtension(project).globalSemaphore.acquireUninterruptibly(requiredPermits);
            logger.info(
                "Acquired {} permits for {} took {} seconds",
                requiredPermits, project.getPath(), Duration.between(startedAt, Instant.now())
            );

            getSelfAsExtension(project).cleanupThread.watch(
                startedClusters.get()
            );
        });
    }

    public static void releasePermits(Project project, int permitsToRelease, Supplier<Collection<ElasticsearchCluster>> stoppedClusters) {
        TestClustersRateLimitExtension selfAsExtension = getSelfAsExtension(project);
        selfAsExtension.cleanupThread.unWatch(stoppedClusters.get());
        logger.info("Will release {} permits for {}", permitsToRelease, project.getPath());
        selfAsExtension.globalSemaphore.release(permitsToRelease);
    }

    public static void createExtension(Project project) {
        if (getSelfAsExtension(project) != null) {
            return;
        }
        // Configure the extension on the root project so we have a single instance per run
        TestClustersRateLimitExtension newExt = project.getRootProject().getExtensions().create(
            "__testclusters_rate_limit",
            TestClustersRateLimitExtension.class,
            maxPermits(project)
        );
        Thread shutdownHook = new Thread(newExt.cleanupThread::run);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        project.getGradle().buildFinished(buildResult -> {
            newExt.executorService.shutdownNow();
            try {
                if (newExt.executorService.awaitTermination(EXECUTOR_SHUTDOWN_TIMEOUT, EXECUTOR_SHUTDOWN_TIMEOUT_UNIT) == false) {
                    throw new IllegalStateException(
                        "Failed to shut down executor service after " +
                            EXECUTOR_SHUTDOWN_TIMEOUT + " " + EXECUTOR_SHUTDOWN_TIMEOUT_UNIT
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            try {
                if (false == Runtime.getRuntime().removeShutdownHook(shutdownHook)) {
                    logger.warn("Trying to deregister shutdown hook when it was not registered.");
                }
            } catch (IllegalStateException ese) {
                // Thrown when shutdown is in progress
                logger.warn("Can't remove shutdown hook", ese);
            }
        });
    }

    private static int maxPermits(Project project) {
        return Optional.ofNullable(System.getProperty("testclusters.max-nodes"))
            .map(Integer::valueOf)
            .orElse(
                project.getGradle().getStartParameter().getMaxWorkerCount()
            );
    }

    private static TestClustersRateLimitExtension getSelfAsExtension(Project project) {
        return project.getRootProject().getExtensions().findByType(TestClustersRateLimitExtension.class);
    }

}
