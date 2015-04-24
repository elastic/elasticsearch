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
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import com.google.common.base.Predicate;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.DjbHashFunction;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.test.cache.recycler.MockBigArrays;
import org.elasticsearch.test.cache.recycler.MockPageCacheRecycler;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.elasticsearch.test.search.MockSearchService;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.*;
import org.junit.rules.RuleChain;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllFilesClosed;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSearchersClosed;

/**
 * Base testcase for randomized unit testing with Elasticsearch
 */
@Listeners({
        ReproduceInfoPrinter.class,
        LoggingListener.class
})
@ThreadLeakScope(Scope.SUITE)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
// we suppress pretty much all the lucene codecs for now, except asserting
// assertingcodec is the winner for a codec here: it finds bugs and gives clear exceptions.
@SuppressCodecs({
        "SimpleText", "Memory", "CheapBastard", "Direct", "Compressing", "FST50", "FSTOrd50",
        "TestBloomFilteredLucenePostings", "MockRandom", "BlockTreeOrds", "LuceneFixedGap",
        "LuceneVarGapFixedInterval", "LuceneVarGapDocFreqInterval", "Lucene50"
})
@LuceneTestCase.SuppressReproduceLine
public abstract class ElasticsearchTestCase extends LuceneTestCase {

    static {
        SecurityHack.ensureInitialized();
    }

    protected final ESLogger logger = Loggers.getLogger(getClass());

    // -----------------------------------------------------------------
    // Suite and test case setup/cleanup.
    // -----------------------------------------------------------------

    @Rule
    public RuleChain failureAndSuccessEvents = RuleChain.outerRule(new TestRuleAdapter() {
        @Override
        protected void afterIfSuccessful() throws Throwable {
            ElasticsearchTestCase.this.afterIfSuccessful();
        }

        @Override
        protected void afterAlways(List<Throwable> errors) throws Throwable {
            if (errors != null && errors.isEmpty() == false) {
                ElasticsearchTestCase.this.afterIfFailed(errors);
            }
            super.afterAlways(errors);
        }
    });

    /** called when a test fails, supplying the errors it generated */
    protected void afterIfFailed(List<Throwable> errors) {
    }

    /** called after a test is finished, but only if succesfull */
    protected void afterIfSuccessful() {
    }

    // TODO: Parent/child and other things does not work with the query cache
    // We must disable query cache for both suite and test to override lucene, but LTC resets it after the suite

    @BeforeClass
    public static void disableQueryCacheSuite() {
        IndexSearcher.setDefaultQueryCache(null);
    }

    @Before
    public final void disableQueryCache() {
        IndexSearcher.setDefaultQueryCache(null);
    }

    // setup mock filesystems for this test run. we change PathUtils
    // so that all accesses are plumbed thru any mock wrappers

    @BeforeClass
    public static void setFileSystem() throws Exception {
        Field field = PathUtils.class.getDeclaredField("DEFAULT");
        field.setAccessible(true);
        FileSystem mock = LuceneTestCase.getBaseTempDirForTestClass().getFileSystem();
        field.set(null, mock);
        assertEquals(mock, PathUtils.getDefaultFileSystem());
    }

    @AfterClass
    public static void restoreFileSystem() throws Exception {
        Field field1 = PathUtils.class.getDeclaredField("ACTUAL_DEFAULT");
        field1.setAccessible(true);
        Field field2 = PathUtils.class.getDeclaredField("DEFAULT");
        field2.setAccessible(true);
        field2.set(null, field1.get(null));
    }

    // setup a default exception handler which knows when and how to print a stacktrace
    private static Thread.UncaughtExceptionHandler defaultHandler;

    @BeforeClass
    public static void setDefaultExceptionHandler() throws Exception {
        defaultHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler(defaultHandler));
    }

    @AfterClass
    public static void restoreDefaultExceptionHandler() throws Exception {
        Thread.setDefaultUncaughtExceptionHandler(defaultHandler);
    }

    // randomize content type for request builders

    @BeforeClass
    public static void setContentType() throws Exception {
        Requests.CONTENT_TYPE = randomFrom(XContentType.values());
        Requests.INDEX_CONTENT_TYPE = randomFrom(XContentType.values());
    }

    @AfterClass
    public static void restoreContentType() {
        Requests.CONTENT_TYPE = XContentType.SMILE;
        Requests.INDEX_CONTENT_TYPE = XContentType.JSON;
    }

    // randomize and override the number of cpus so tests reproduce regardless of real number of cpus

    @BeforeClass
    public static void setProcessors() {
        int numCpu = TestUtil.nextInt(random(), 1, 4);
        System.setProperty(EsExecutors.DEFAULT_SYSPROP, Integer.toString(numCpu));
        assertEquals(numCpu, EsExecutors.boundedNumberOfProcessors(ImmutableSettings.EMPTY));
    }

    @AfterClass
    public static void restoreProcessors() {
        System.clearProperty(EsExecutors.DEFAULT_SYSPROP);
    }

    // check some things (like MockDirectoryWrappers) are closed where we currently
    // manage them. TODO: can we add these to LuceneTestCase.closeAfterSuite directly?
    // or something else simpler instead of the fake closeables?

    @BeforeClass
    public static void setAfterSuiteAssertions() throws Exception {
        closeAfterSuite(new Closeable() {
            @Override
            public void close() throws IOException {
                assertAllFilesClosed();
            }
        });
        closeAfterSuite(new Closeable() {
            @Override
            public void close() throws IOException {
                assertAllSearchersClosed();
            }
        });
    }

    @After
    public final void ensureCleanedUp() throws Exception {
        MockPageCacheRecycler.ensureAllPagesAreReleased();
        MockBigArrays.ensureAllArraysAreReleased();
        // field cache should NEVER get loaded.
        String[] entries = UninvertingReader.getUninvertedStats();
        assertEquals("fieldcache must never be used, got=" + Arrays.toString(entries), 0, entries.length);
    }

    // this must be a separate method from other ensure checks above so suite scoped integ tests can call...TODO: fix that
    @After
    public final void ensureAllSearchContextsReleased() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                MockSearchService.assertNoInFLightContext();
            }
        });
    }

    // mockdirectorywrappers currently set this boolean if checkindex fails
    // TODO: can we do this cleaner???

    /** MockFSDirectoryService sets this: */
    public static boolean checkIndexFailed;

    @Before
    public final void resetCheckIndexStatus() throws Exception {
        checkIndexFailed = false;
    }

    @After
    public final void ensureCheckIndexPassed() throws Exception {
        assertFalse("at least one shard failed CheckIndex", checkIndexFailed);
    }

    // -----------------------------------------------------------------
    // Test facilities and facades for subclasses. 
    // -----------------------------------------------------------------

    // TODO: replaces uses of getRandom() with random()
    // TODO: decide on one set of naming for between/scaledBetween and remove others
    // TODO: replace frequently() with usually()

    /** Shortcut for {@link RandomizedContext#getRandom()}. Use {@link #random()} instead. */
    public static Random getRandom() {
        // TODO: replace uses of this function with random()
        return random();
    }

    /**
     * Returns a "scaled" random number between min and max (inclusive).
     *
     * @see RandomizedTest#scaledRandomIntBetween(int, int);
     */
    public static int scaledRandomIntBetween(int min, int max) {
        return RandomizedTest.scaledRandomIntBetween(min, max);
    }

    /**
     * A random integer from <code>min</code> to <code>max</code> (inclusive).
     *
     * @see #scaledRandomIntBetween(int, int)
     */
    public static int randomIntBetween(int min, int max) {
        return RandomInts.randomIntBetween(random(), min, max);
    }

    /**
     * Returns a "scaled" number of iterations for loops which can have a variable
     * iteration count. This method is effectively
     * an alias to {@link #scaledRandomIntBetween(int, int)}.
     */
    public static int iterations(int min, int max) {
        return scaledRandomIntBetween(min, max);
    }

    /**
     * An alias for {@link #randomIntBetween(int, int)}.
     *
     * @see #scaledRandomIntBetween(int, int)
     */
    public static int between(int min, int max) {
        return randomIntBetween(min, max);
    }

    /**
     * The exact opposite of {@link #rarely()}.
     */
    public static boolean frequently() {
        return !rarely();
    }

    public static boolean randomBoolean() {
        return random().nextBoolean();
    }

    public static byte randomByte() {
        return (byte) random().nextInt();
    }

    public static short randomShort() {
        return (short) random().nextInt();
    }

    public static int randomInt() {
        return random().nextInt();
    }

    public static float randomFloat() {
        return random().nextFloat();
    }

    public static double randomDouble() {
        return random().nextDouble();
    }

    public static long randomLong() {
        return random().nextLong();
    }

    /** A random integer from 0..max (inclusive). */
    public static int randomInt(int max) {
        return RandomizedTest.randomInt(max);
    }

    /** Pick a random object from the given array. The array must not be empty. */
    public static <T> T randomFrom(T... array) {
        return RandomPicks.randomFrom(random(), array);
    }

    /** Pick a random object from the given list. */
    public static <T> T randomFrom(List<T> list) {
        return RandomPicks.randomFrom(random(), list);
    }

    public static String randomAsciiOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        return RandomizedTest.randomAsciiOfLengthBetween(minCodeUnits, maxCodeUnits);
    }

    public static String randomAsciiOfLength(int codeUnits) {
        return RandomizedTest.randomAsciiOfLength(codeUnits);
    }

    public static String randomUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        return RandomizedTest.randomUnicodeOfLengthBetween(minCodeUnits, maxCodeUnits);
    }

    public static String randomUnicodeOfLength(int codeUnits) {
        return RandomizedTest.randomUnicodeOfLength(codeUnits);
    }

    public static String randomUnicodeOfCodepointLengthBetween(int minCodePoints, int maxCodePoints) {
        return RandomizedTest.randomUnicodeOfCodepointLengthBetween(minCodePoints, maxCodePoints);
    }

    public static String randomUnicodeOfCodepointLength(int codePoints) {
        return RandomizedTest.randomUnicodeOfCodepointLength(codePoints);
    }

    public static String randomRealisticUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        return RandomizedTest.randomRealisticUnicodeOfLengthBetween(minCodeUnits, maxCodeUnits);
    }

    public static String randomRealisticUnicodeOfLength(int codeUnits) {
        return RandomizedTest.randomRealisticUnicodeOfLength(codeUnits);
    }

    public static String randomRealisticUnicodeOfCodepointLengthBetween(int minCodePoints, int maxCodePoints) {
        return RandomizedTest.randomRealisticUnicodeOfCodepointLengthBetween(minCodePoints, maxCodePoints);
    }

    public static String randomRealisticUnicodeOfCodepointLength(int codePoints) {
        return RandomizedTest.randomRealisticUnicodeOfCodepointLength(codePoints);
    }

    public static String[] generateRandomStringArray(int maxArraySize, int maxStringSize, boolean allowNull) {
        if (allowNull && random().nextBoolean()) {
            return null;
        }
        String[] array = new String[random().nextInt(maxArraySize)]; // allow empty arrays
        for (int i = 0; i < array.length; i++) {
            array[i] = RandomStrings.randomAsciiOfLength(random(), maxStringSize);
        }
        return array;
    }

    /**
     * Runs the code block for 10 seconds waiting for no assertion to trip.
     */
    public static void assertBusy(Runnable codeBlock) throws Exception {
        assertBusy(Executors.callable(codeBlock), 10, TimeUnit.SECONDS);
    }

    public static void assertBusy(Runnable codeBlock, long maxWaitTime, TimeUnit unit) throws Exception {
        assertBusy(Executors.callable(codeBlock), maxWaitTime, unit);
    }

    /**
     * Runs the code block for 10 seconds waiting for no assertion to trip.
     */
    public static <V> V assertBusy(Callable<V> codeBlock) throws Exception {
        return assertBusy(codeBlock, 10, TimeUnit.SECONDS);
    }

    /**
     * Runs the code block for the provided interval, waiting for no assertions to trip.
     */
    public static <V> V assertBusy(Callable<V> codeBlock, long maxWaitTime, TimeUnit unit) throws Exception {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long iterations = Math.max(Math.round(Math.log10(maxTimeInMillis) / Math.log10(2)), 1);
        long timeInMillis = 1;
        long sum = 0;
        List<AssertionError> failures = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            try {
                return codeBlock.call();
            } catch (AssertionError e) {
                failures.add(e);
            }
            sum += timeInMillis;
            Thread.sleep(timeInMillis);
            timeInMillis *= 2;
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        try {
            return codeBlock.call();
        } catch (AssertionError e) {
            for (AssertionError failure : failures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

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

    public static boolean terminate(ExecutorService... services) throws InterruptedException {
        boolean terminated = true;
        for (ExecutorService service : services) {
            if (service != null) {
                terminated &= ThreadPool.terminate(service, 10, TimeUnit.SECONDS);
            }
        }
        return terminated;
    }

    public static boolean terminate(ThreadPool service) throws InterruptedException {
        return ThreadPool.terminate(service, 10, TimeUnit.SECONDS);
    }

    /**
     * Returns a {@link java.nio.file.Path} pointing to the class path relative resource given
     * as the first argument. In contrast to
     * <code>getClass().getResource(...).getFile()</code> this method will not
     * return URL encoded paths if the parent path contains spaces or other
     * non-standard characters.
     */
    @Override
    public Path getDataPath(String relativePath) {
        // we override LTC behavior here: wrap even resources with mockfilesystems, 
        // because some code is buggy when it comes to multiple nio.2 filesystems
        // (e.g. FileSystemUtils, and likely some tests)
        try {
            return PathUtils.get(getClass().getResource(relativePath).toURI());
        } catch (Exception e) {
            throw new RuntimeException("resource not found: " + relativePath, e);
        }
    }

    /** Returns a random number of temporary paths. */
    public String[] tmpPaths() {
        final int numPaths = TestUtil.nextInt(random(), 1, 3);
        final String[] absPaths = new String[numPaths];
        for (int i = 0; i < numPaths; i++) {
            absPaths[i] = createTempDir().toAbsolutePath().toString();
        }
        return absPaths;
    }

    public NodeEnvironment newNodeEnvironment() throws IOException {
        return newNodeEnvironment(ImmutableSettings.EMPTY);
    }

    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = ImmutableSettings.builder()
                .put(settings)
                .put("path.home", createTempDir().toAbsolutePath())
                .putArray("path.data", tmpPaths()).build();
        return new NodeEnvironment(build, new Environment(build));
    }

    /** Return consistent index settings for the provided index version. */
    public static ImmutableSettings.Builder settings(Version version) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version);
        if (version.before(Version.V_2_0_0)) {
            builder.put(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION, DjbHashFunction.class);
        }
        return builder;
    }

    // -----------------------------------------------------------------
    // Failure utilities
    // -----------------------------------------------------------------

    static final class ElasticsearchUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {

        private final Thread.UncaughtExceptionHandler parent;
        private final ESLogger logger = Loggers.getLogger(getClass());

        private ElasticsearchUncaughtExceptionHandler(Thread.UncaughtExceptionHandler parent) {
            this.parent = parent;
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            if (e instanceof EsRejectedExecutionException) {
                if (e.getMessage() != null && e.getMessage().contains(EsAbortPolicy.SHUTTING_DOWN_KEY)) {
                    return; // ignore the EsRejectedExecutionException when a node shuts down
                }
            } else if (e instanceof OutOfMemoryError) {
                if (e.getMessage() != null && e.getMessage().contains("unable to create new native thread")) {
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

    /** Dump threads and their current stack trace. */
    private static String formatThreadStacks(Map<Thread, StackTraceElement[]> threads) {
        StringBuilder message = new StringBuilder();
        int cnt = 1;
        final Formatter f = new Formatter(message, Locale.ENGLISH);
        for (Map.Entry<Thread, StackTraceElement[]> e : threads.entrySet()) {
            if (e.getKey().isAlive()) {
                f.format(Locale.ENGLISH, "\n  %2d) %s", cnt++, threadName(e.getKey())).flush();
            }
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
