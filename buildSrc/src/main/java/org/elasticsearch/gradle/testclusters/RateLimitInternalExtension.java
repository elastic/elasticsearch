package org.elasticsearch.gradle.testclusters;

import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * This extensions was meant to be used internally by testclusters
 *
 * It holds synchronization primitives needed to implement the rate limiting.
 * This is tricky because we can't use Gradle workers as there's no way to make sure that tests and their clusters are
 * allocated atomically, so we could be in a situation where all workers are tests waiting for clusters to start up.
 *
 * Also auto configures cleanup of executors to make sure we don't leak threads in the daemon.
 */
public class RateLimitInternalExtension {

    private static final int EXECUTOR_SHUTDOWN_TIMEOUT = 1;
    private static final TimeUnit EXECUTOR_SHUTDOWN_TIMEOUT_UNIT = TimeUnit.MINUTES;

    private static final Logger logger =  Logging.getLogger(RateLimitInternalExtension.class);

    private final Semaphore globalSemaphore;
    private final ExecutorService executorService;
    private final TestClusterCleanupOnShutdown cleanupThread;

    public RateLimitInternalExtension() {
        globalSemaphore = new Semaphore(maxPermits());
        executorService = Executors.newFixedThreadPool(
            RateLimitInternalExtension.maxPermits() +
                1
        );
        cleanupThread = new TestClusterCleanupOnShutdown();
        executorService.submit(cleanupThread);
    }

    public static Semaphore semaphore(Project project) {
        return getSelfAsExtension(project).globalSemaphore;
    }

    public static ExecutorService executorService(Project project) {
        return getSelfAsExtension(project).executorService;
    }

    public static TestClusterCleanupOnShutdown cleanupThread(Project project) {
        return getSelfAsExtension(project).cleanupThread;
    }

    static int maxPermits() {
        return Optional.ofNullable(System.getProperty("testclusters.max-nodes"))
            .map(Integer::valueOf)
            .orElse(Runtime.getRuntime().availableProcessors() / 2);
    }

    private static RateLimitInternalExtension getSelfAsExtension(Project project) {
        RateLimitInternalExtension ext = project.getRootProject().getExtensions().findByType(RateLimitInternalExtension.class);
        if (ext == null) {
            // Configure the extension on the root project so we have a single instance per run
            RateLimitInternalExtension newExt = project.getRootProject().getExtensions().create(
                "__testclusters_rate_limit"  + UUID.randomUUID().toString(),
                RateLimitInternalExtension.class
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
            return newExt;
        }
        return ext;
    }

}
