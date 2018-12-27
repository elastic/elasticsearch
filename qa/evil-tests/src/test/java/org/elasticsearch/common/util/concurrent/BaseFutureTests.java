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
package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class BaseFutureTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() {
        threadPool = new TestThreadPool(BaseFutureTests.class.getName());
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testErrorCaught() throws InterruptedException {
        Consumer<Throwable> expected = t -> {
            assertThat(t, instanceOf(Error.class));
            assertThat(t, hasToString(containsString("future error")));
        };

        runFutureTest(
            () -> new BaseFuture<>().completeExceptionally(new Error("future error")),
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.exceptionally(t -> {
                    throw new Error("future error");
                });
                fut.completeExceptionally(new RuntimeException("test"));
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.thenRun(() -> {
                    throw new Error("future error");
                });
                fut.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.thenRunAsync(() -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.thenApply(o -> {
                    throw new Error("future error");
                });
                fut.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.thenApplyAsync(o -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.handle((o, t) -> {
                    throw new Error("future error");
                });
                fut.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut = new BaseFuture<>();
                fut.handleAsync((o, t) -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.acceptEither(fut2, o -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.acceptEither(fut2, o -> {
                    throw new Error("future error");
                });
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.acceptEitherAsync(fut2, o -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.acceptEitherAsync(fut2, o -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.applyToEither(fut2, o -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.applyToEither(fut2, o -> {
                    throw new Error("future error");
                });
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.applyToEitherAsync(fut2, o -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.applyToEitherAsync(fut2, o -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                fut1.whenComplete((o, t) -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                fut1.whenCompleteAsync((o, t) -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                fut1.thenAccept(o -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                fut1.thenAcceptAsync(o -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.runAfterBoth(fut2, () -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.runAfterBothAsync(fut2, () -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.runAfterEither(fut2, () -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.runAfterEither(fut2, () -> {
                    throw new Error("future error");
                });
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.runAfterEitherAsync(fut2, () -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.runAfterEitherAsync(fut2, () -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.thenCombine(fut2, (o1, o2) -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.thenCombineAsync(fut2, (o1, o2) -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.thenAcceptBoth(fut2, (o1, o2) -> {
                    throw new Error("future error");
                });
                fut1.complete(new Object());
                fut2.complete(new Object());
            },
            expected);

        runFutureTest(
            () -> {
                BaseFuture<Object> fut1 = new BaseFuture<>();
                BaseFuture<Object> fut2 = new BaseFuture<>();
                fut1.thenAcceptBothAsync(fut2, (o1, o2) -> {
                    throw new Error("future error");
                }, threadPool.generic());
                fut1.complete(new Object());
                fut2.complete(new Object());
            },
            expected);
    }

    private void runFutureTest(final Runnable runnable, final Consumer<Throwable> consumer) throws InterruptedException {
        final AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        final Thread.UncaughtExceptionHandler uncaughtExceptionHandler = Thread.getDefaultUncaughtExceptionHandler();
        final CountDownLatch uncaughtExceptionHandlerLatch = new CountDownLatch(1);

        try {
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> {
                throwableReference.set(e);
                uncaughtExceptionHandlerLatch.countDown();
            });

            runnable.run();

            uncaughtExceptionHandlerLatch.await();
            consumer.accept(throwableReference.get());
        } finally {
            Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
        }
    }

}
