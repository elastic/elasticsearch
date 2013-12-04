/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.annotations.*;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.AbstractRandomizedTest;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.elasticsearch.test.engine.MockRobinEngine;
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

/**
 * Base testcase for randomized unit testing with Elasticsearch
 */
@ThreadLeakFilters(defaultFilters = true, filters = {ElasticsearchThreadFilter.class})
@ThreadLeakScope(Scope.NONE)
@TimeoutSuite(millis = TimeUnits.HOUR) // timeout the suite after 1h and fail the test.
@Listeners(LoggingListener.class)
public abstract class ElasticsearchTestCase extends AbstractRandomizedTest {

    private static Thread.UncaughtExceptionHandler defaultHandler;
    
    protected final ESLogger logger = Loggers.getLogger(getClass());

    public static final String CHILD_VM_ID = System.getProperty("junit4.childvm.id", "" + System.currentTimeMillis());
    
    public static boolean awaitBusy(Predicate<?> breakPredicate) throws InterruptedException {
        return awaitBusy(breakPredicate, 10, TimeUnit.SECONDS);
    }
    
    public static boolean awaitBusy(Predicate<?> breakPredicate, long maxWaitTime, TimeUnit unit) throws InterruptedException {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long iterations = Math.max(Math.round(Math.log10(maxTimeInMillis) / Math.log10(2)), 1);
        long timeInMillis = 1;
        long sum = 0;
        for (int i = 0; i < iterations; i++) {
            if (breakPredicate.apply(null)) {
                return true;
            }
            sum += timeInMillis;
            Thread.sleep(timeInMillis);
            timeInMillis *= 2;
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        return breakPredicate.apply(null);
    }
    
    private static final String[] numericTypes = new String[] {"byte", "short", "integer", "long"};
    
    public static String randomNumericType(Random random) {
        return numericTypes[random.nextInt(numericTypes.length)];
    }
    
    /**
     * Returns a {@link File} pointing to the class path relative resource given
     * as the first argument. In contrast to
     * <code>getClass().getResource(...).getFile()</code> this method will not
     * return URL encoded paths if the parent path contains spaces or other
     * non-standard characters.
     */
    public File getResource(String relativePath) {
        URI uri = URI.create(getClass().getResource(relativePath).toString());
        return new File(uri);
    }
    
    public static void ensureAllFilesClosed() throws IOException {
        try {
            for (MockDirectoryHelper.ElasticsearchMockDirectoryWrapper w : MockDirectoryHelper.wrappers) {
                if (w.isOpen()) {
                    w.closeWithRuntimeException();
                }
            }
        } finally {
            forceClearMockWrappers();
        }
    }
    
    public static void ensureAllSearchersClosed() {
        /* in some cases we finish a test faster than the freeContext calls make it to the
         * shards. Let's wait for some time if there are still searchers. If the are really 
         * pending we will fail anyway.*/
        try {
            if (awaitBusy(new Predicate<Object>() {
                public boolean apply(Object o) {
                    return MockRobinEngine.INFLIGHT_ENGINE_SEARCHERS.isEmpty();
                }
            }, 5, TimeUnit.SECONDS)) {
                return;
            }
        } catch (InterruptedException ex) {
            if (MockRobinEngine.INFLIGHT_ENGINE_SEARCHERS.isEmpty()) {
                return;
            }
        }
        try {
            RuntimeException ex = null;
            StringBuilder builder = new StringBuilder("Unclosed Searchers instance for shards: [");
            for (Entry<MockRobinEngine.AssertingSearcher, RuntimeException> entry : MockRobinEngine.INFLIGHT_ENGINE_SEARCHERS.entrySet()) {
                ex = entry.getValue();
                builder.append(entry.getKey().shardId()).append(",");
            }
            builder.append("]");
            throw new RuntimeException(builder.toString(), ex);
        } finally {
            MockRobinEngine.INFLIGHT_ENGINE_SEARCHERS.clear();
        }
    }
    
    public static void forceClearMockWrappers() {
        MockDirectoryHelper.wrappers.clear();
    }
    
    public static boolean hasUnclosedWrapper() {
        for (MockDirectoryWrapper w : MockDirectoryHelper.wrappers) {
            if (w.isOpen()) {
                return true;
            }
        }
        return false;
    }

    
    @BeforeClass
    public static void registerMockDirectoryHooks() throws Exception {
        closeAfterSuite(new Closeable() {
            @Override
            public void close() throws IOException {
                ensureAllFilesClosed();
            }
        });
        
        closeAfterSuite(new Closeable() {
            @Override
            public void close() throws IOException {
                ensureAllSearchersClosed();
            }
        });
        defaultHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler(defaultHandler));
    }

    @AfterClass
    public static void resetUncaughtExceptionHandler() {
       Thread.setDefaultUncaughtExceptionHandler(defaultHandler);
    }
    
    private static final List<Version> SORTED_VERSIONS;
    
    static {
        Field[] declaredFields = Version.class.getDeclaredFields();
        Set<Integer> ids = new HashSet<Integer>();
        for (Field field : declaredFields) {
            final int mod = field.getModifiers();
            if (Modifier.isStatic(mod) && Modifier.isFinal(mod) && Modifier.isPublic(mod)) {
                if (field.getType() == Version.class) {
                    try {
                        Version object = (Version) field.get(null);
                        ids.add(object.id);
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        List<Integer> idList = new ArrayList<Integer>(ids);
        Collections.sort(idList);
        Collections.reverse(idList);
        ImmutableList.Builder<Version> version = ImmutableList.builder();
        for (Integer integer : idList) {
            version.add(Version.fromId(integer));
        }
        SORTED_VERSIONS = version.build();
    }
    
    public static Version getPreviousVersion() {
        Version version = SORTED_VERSIONS.get(1);
        assert version.before(Version.CURRENT);
        return version;
    }
    
    public static Version randomVersion() {
        return randomVersion(getRandom());
    }
    
    public static Version randomVersion(Random random) {
        return SORTED_VERSIONS.get(random.nextInt(SORTED_VERSIONS.size()));
    }

    static final class ElasticsearchUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final Thread.UncaughtExceptionHandler parent;
        private final ESLogger logger = Loggers.getLogger(getClass());

        private ElasticsearchUncaughtExceptionHandler(Thread.UncaughtExceptionHandler parent) {
            this.parent = parent;
        }


        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (e instanceof EsRejectedExecutionException) {
                if (e.getMessage().contains(EsAbortPolicy.SHUTTING_DOWN_KEY)) {
                    return; // ignore the EsRejectedExecutionException when a node shuts down
                }
            } else if (e instanceof OutOfMemoryError) {
                if (e.getMessage().contains("unable to create new native thread")) {
                   printStackDump(logger);
                }
            }
            parent.uncaughtException(t, e);
        }

    }

    protected static final void printStackDump(ESLogger logger) {
        // print stack traces if we can't create any native thread anymore
        Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
        logger.error(formatThreadStacks(allStackTraces));
    }

    /**
     * Dump threads and their current stack trace.
     */
    private static String formatThreadStacks(Map<Thread,StackTraceElement[]> threads) {
        StringBuilder message = new StringBuilder();
        int cnt = 1;
        final Formatter f = new Formatter(message, Locale.ENGLISH);
        for (Map.Entry<Thread,StackTraceElement[]> e : threads.entrySet()) {
            if (e.getKey().isAlive())
                f.format(Locale.ENGLISH, "\n  %2d) %s", cnt++, threadName(e.getKey())).flush();
            if (e.getValue().length == 0) {
                message.append("\n        at (empty stack)");
            } else {
                for (StackTraceElement ste : e.getValue()) {
                    message.append("\n        at ").append(ste);
                }
            }
        }
        return message.toString();
    }

    private static String threadName(Thread t) {
        return "Thread[" +
                "id=" + t.getId() +
                ", name=" + t.getName() +
                ", state=" + t.getState() +
                ", group=" + groupName(t.getThreadGroup()) +
                "]";
    }

    private static String groupName(ThreadGroup threadGroup) {
        if (threadGroup == null) {
            return "{null group}";
        } else {
            return threadGroup.getName();
        }
    }


}
