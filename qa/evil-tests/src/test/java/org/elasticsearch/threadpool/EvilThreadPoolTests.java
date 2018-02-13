package org.elasticsearch.threadpool;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class EvilThreadPoolTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(EvilThreadPoolTests.class.getName());
    }

    @After
    public void tearDownThreadPool() throws InterruptedException {
        terminate(threadPool);
    }

    public void testExecutionException() throws InterruptedException {
        runExecutionExceptionTest(
                () -> {
                    throw new Error("future error");
                },
                t -> {
                    assertThat(t, instanceOf(Error.class));
                    assertThat(t, hasToString(containsString("future error")));
                });
        runExecutionExceptionTest(
                () -> {
                    throw new IllegalStateException("future exception");
                },
                t -> {
                    assertThat(t, instanceOf(RuntimeException.class));
                    assertNotNull(t.getCause());
                    assertThat(t.getCause(), instanceOf(IllegalStateException.class));
                    assertThat(t.getCause(), hasToString(containsString("future exception")));
                }
        );
    }

    private void runExecutionExceptionTest(final Supplier<Throwable> supplier, final Consumer<Throwable> consumer) throws InterruptedException {
        final AtomicReference<Throwable> maybeThrowable = new AtomicReference<>();
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                maybeThrowable.set(e);
                latch.countDown();
            });

            threadPool.generic().submit(supplier::get);

            latch.await();
            assertNotNull(maybeThrowable.get());
            consumer.accept(maybeThrowable.get());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

}
