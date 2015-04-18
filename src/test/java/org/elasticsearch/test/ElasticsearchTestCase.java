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

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SysGlobals;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
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
import org.elasticsearch.test.store.MockDirectoryHelper;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
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
@Ignore
@SuppressCodecs({"SimpleText", "Memory", "CheapBastard", "Direct"}) // slow ones
@LuceneTestCase.SuppressReproduceLine
public abstract class ElasticsearchTestCase extends LuceneTestCase {
    
    static {
        SecurityHack.ensureInitialized();
    }
    
    // setup mock filesystems for this test run. we change PathUtils
    // so that all accesses are plumbed thru any mock wrappers
    
    @BeforeClass
    public static void setUpFileSystem() {
        try {
            Field field = PathUtils.class.getDeclaredField("DEFAULT");
            field.setAccessible(true);
            field.set(null, LuceneTestCase.getBaseTempDirForTestClass().getFileSystem());
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException();
        }
    }
    
    @AfterClass
    public static void restoreFileSystem() {
        try {
            Field field1 = PathUtils.class.getDeclaredField("ACTUAL_DEFAULT");
            field1.setAccessible(true);
            Field field2 = PathUtils.class.getDeclaredField("DEFAULT");
            field2.setAccessible(true);
            field2.set(null, field1.get(null));
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException();
        }
    }
    
    @BeforeClass
    public static void setUpProcessors() {
        int numCpu = TestUtil.nextInt(random(), 1, 4);
        System.setProperty(EsExecutors.DEFAULT_SYSPROP, Integer.toString(numCpu));
        assertEquals(numCpu, EsExecutors.boundedNumberOfProcessors(ImmutableSettings.EMPTY));
    }

    @AfterClass
    public static void restoreProcessors() {
        System.clearProperty(EsExecutors.DEFAULT_SYSPROP);
    }

    @Before
    public void disableQueryCache() {
        // TODO: Parent/child and other things does not work with the query cache
        IndexSearcher.setDefaultQueryCache(null);
    }

    @After
    public void ensureNoFieldCacheUse() {
        // field cache should NEVER get loaded.
        String[] entries = UninvertingReader.getUninvertedStats();
        assertEquals("fieldcache must never be used, got=" + Arrays.toString(entries), 0, entries.length);
    }
    
    // old shit:
    
    /**
     * The child JVM ordinal of this JVM. Default is <tt>0</tt>
     */
    public static final int CHILD_JVM_ID = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));

    /**
     * Annotation for REST tests
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @TestGroup(enabled = true, sysProperty = TESTS_REST)
    public @interface Rest {
    }

    /**
     * Property that allows to control whether the REST tests are run (default) or not
     */
    public static final String TESTS_REST = "tests.rest";

    /**
     * Annotation for integration tests
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @TestGroup(enabled = true, sysProperty = SYSPROP_INTEGRATION)
    public @interface Integration {
    }

    // --------------------------------------------------------------------
    // Test groups, system properties and other annotations modifying tests
    // --------------------------------------------------------------------

    /**
     * @see #ignoreAfterMaxFailures
     */
    public static final String SYSPROP_MAXFAILURES = "tests.maxfailures";

    /**
     * @see #ignoreAfterMaxFailures
     */
    public static final String SYSPROP_FAILFAST = "tests.failfast";

    public static final String SYSPROP_INTEGRATION = "tests.integration";
    // -----------------------------------------------------------------
    // Suite and test case setup/ cleanup.
    // -----------------------------------------------------------------

    /** MockFSDirectoryService sets this: */
    public static boolean checkIndexFailed;

    /**
     * For subclasses to override. Overrides must call {@code super.setUp()}.
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();
        checkIndexFailed = false;
    }

    /**
     * For subclasses to override. Overrides must call {@code super.tearDown()}.
     */
    @After
    public void tearDown() throws Exception {
        assertFalse("at least one shard failed CheckIndex", checkIndexFailed);
        super.tearDown();
    }


    // -----------------------------------------------------------------
    // Test facilities and facades for subclasses. 
    // -----------------------------------------------------------------
    
    // old helper stuff, a lot of it is bad news and we should see if its all used
    
    /**
     * Returns a "scaled" random number between min and max (inclusive).
     * @see RandomizedTest#scaledRandomIntBetween(int, int);
     */
    public static int scaledRandomIntBetween(int min, int max) {
        return RandomizedTest.scaledRandomIntBetween(min, max);
    }
    
    /** 
     * A random integer from <code>min</code> to <code>max</code> (inclusive).
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
    public static byte    randomByte()     { return (byte) getRandom().nextInt(); }
    public static short   randomShort()    { return (short) getRandom().nextInt(); }
    public static int     randomInt()      { return getRandom().nextInt(); }
    public static float   randomFloat()    { return getRandom().nextFloat(); }
    public static double  randomDouble()   { return getRandom().nextDouble(); }
    public static long    randomLong()     { return getRandom().nextLong(); }
    
    /**
     * Pick a random object from the given array. The array must not be empty.
     */
    public static <T> T randomFrom(T... array) {
      return RandomPicks.randomFrom(random(), array);
    }

    /**
     * Pick a random object from the given list.
     */
    public static <T> T randomFrom(List<T> list) {
      return RandomPicks.randomFrom(random(), list);
    }
    
    /**
     * Shortcut for {@link RandomizedContext#getRandom()}. Even though this method
     * is static, it returns per-thread {@link Random} instance, so no race conditions
     * can occur.
     * 
     * <p>It is recommended that specific methods are used to pick random values.
     */
    public static Random getRandom() {
        return random();
    }
    
    /** 
     * A random integer from 0..max (inclusive). 
     */
    public static int randomInt(int max) { 
      return RandomizedTest.randomInt(max); 
    }

    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomAsciiOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
      return RandomizedTest.randomAsciiOfLengthBetween(minCodeUnits, maxCodeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomAsciiOfLength(int codeUnits) {
      return RandomizedTest.randomAsciiOfLength(codeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
      return RandomizedTest.randomUnicodeOfLengthBetween(minCodeUnits, maxCodeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomUnicodeOfLength(int codeUnits) {
      return RandomizedTest.randomUnicodeOfLength(codeUnits);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomUnicodeOfCodepointLengthBetween(int minCodePoints, int maxCodePoints) {
      return RandomizedTest.randomUnicodeOfCodepointLengthBetween(minCodePoints, maxCodePoints);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomUnicodeOfCodepointLength(int codePoints) {
      return RandomizedTest.randomUnicodeOfCodepointLength(codePoints);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
      return RandomizedTest.randomRealisticUnicodeOfLengthBetween(minCodeUnits, maxCodeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfLength(int codeUnits) {
      return RandomizedTest.randomRealisticUnicodeOfLength(codeUnits);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfCodepointLengthBetween(int minCodePoints, int maxCodePoints) {
      return RandomizedTest.randomRealisticUnicodeOfCodepointLengthBetween(minCodePoints, maxCodePoints);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfCodepointLength(int codePoints) {
      return RandomizedTest.randomRealisticUnicodeOfCodepointLength(codePoints);
    }
    
    /**
     * Shortcut for {@link RandomizedContext#current()}. 
     */
    public static RandomizedContext getContext() {
      return RandomizedTest.getContext();
    }
    
    /**
     * Returns true if we're running nightly tests.
     * @see Nightly
     */
    public static boolean isNightly() {
      return RandomizedTest.isNightly();
    }
    
    /** 
     * Returns a non-negative random value smaller or equal <code>max</code>.
     * @see RandomizedTest#atMost(int);
     */
    public static int atMost(int max) {
        return RandomizedTest.atMost(max);
    }

    private static Thread.UncaughtExceptionHandler defaultHandler;

    protected final ESLogger logger = Loggers.getLogger(getClass());



    static {
        SecurityHack.ensureInitialized();
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

    private static final String[] numericTypes = new String[]{"byte", "short", "integer", "long"};

    public static String randomNumericType(Random random) {
        return numericTypes[random.nextInt(numericTypes.length)];
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

    @After
    public void ensureAllPagesReleased() throws Exception {
        MockPageCacheRecycler.ensureAllPagesAreReleased();
    }

    @After
    public void ensureAllArraysReleased() throws Exception {
        MockBigArrays.ensureAllArraysAreReleased();
    }

    @After
    public void ensureAllSearchContextsReleased() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                MockSearchService.assertNoInFLightContext();
            }
        });
    }

    @BeforeClass
    public static void setBeforeClass() throws Exception {
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
        defaultHandler = Thread.getDefaultUncaughtExceptionHandler();
        Thread.setDefaultUncaughtExceptionHandler(new ElasticsearchUncaughtExceptionHandler(defaultHandler));
        Requests.CONTENT_TYPE = randomXContentType();
        Requests.INDEX_CONTENT_TYPE = randomXContentType();
    }

    public static XContentType randomXContentType() {
        return randomFrom(XContentType.values());
    }

    @AfterClass
    public static void resetAfterClass() {
        Thread.setDefaultUncaughtExceptionHandler(defaultHandler);
        Requests.CONTENT_TYPE = XContentType.SMILE;
        Requests.INDEX_CONTENT_TYPE = XContentType.JSON;
    }

    private static final List<Version> SORTED_VERSIONS;

    static {
        Field[] declaredFields = Version.class.getDeclaredFields();
        Set<Integer> ids = new HashSet<>();
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
        List<Integer> idList = new ArrayList<>(ids);
        Collections.sort(idList);
        Collections.reverse(idList);
        ImmutableList.Builder<Version> version = ImmutableList.builder();
        for (Integer integer : idList) {
            version.add(Version.fromId(integer));
        }
        SORTED_VERSIONS = version.build();
    }

    /**
     * @return the {@link Version} before the {@link Version#CURRENT}
     */
    public static Version getPreviousVersion() {
        Version version = SORTED_VERSIONS.get(1);
        assert version.before(Version.CURRENT);
        return version;
    }
    
    /**
     * A random {@link Version}.
     *
     * @return a random {@link Version} from all available versions
     */
    public static Version randomVersion() {
        return randomVersion(random());
    }
    
    /**
     * A random {@link Version}.
     * 
     * @param random
     *            the {@link Random} to use to generate the random version
     *
     * @return a random {@link Version} from all available versions
     */
    public static Version randomVersion(Random random) {
        return SORTED_VERSIONS.get(random.nextInt(SORTED_VERSIONS.size()));
    }
    
    /**
     * Returns immutable list of all known versions.
     */
    public static List<Version> allVersions() {
        return Collections.unmodifiableList(SORTED_VERSIONS);
    }

    /**
     * A random {@link Version} from <code>minVersion</code> to
     * <code>maxVersion</code> (inclusive).
     * 
     * @param minVersion
     *            the minimum version (inclusive)
     * @param maxVersion
     *            the maximum version (inclusive)
     * @return a random {@link Version} from <code>minVersion</code> to
     *         <code>maxVersion</code> (inclusive)
     */
    public static Version randomVersionBetween(Version minVersion, Version maxVersion) {
        return randomVersionBetween(random(), minVersion, maxVersion);
    }

    /**
     * A random {@link Version} from <code>minVersion</code> to
     * <code>maxVersion</code> (inclusive).
     * 
     * @param random
     *            the {@link Random} to use to generate the random version
     * @param minVersion
     *            the minimum version (inclusive)
     * @param maxVersion
     *            the maximum version (inclusive)
     * @return a random {@link Version} from <code>minVersion</code> to
     *         <code>maxVersion</code> (inclusive)
     */
    public static Version randomVersionBetween(Random random, Version minVersion, Version maxVersion) {
        int minVersionIndex = SORTED_VERSIONS.size();
        if (minVersion != null) {
            minVersionIndex = SORTED_VERSIONS.indexOf(minVersion);
        }
        int maxVersionIndex = 0;
        if (maxVersion != null) {
            maxVersionIndex = SORTED_VERSIONS.indexOf(maxVersion);
        }
        if (minVersionIndex == -1) {
            throw new IllegalArgumentException("minVersion [" + minVersion + "] does not exist.");
        } else if (maxVersionIndex == -1) {
            throw new IllegalArgumentException("maxVersion [" + maxVersion + "] does not exist.");
        } else {
            // minVersionIndex is inclusive so need to add 1 to this index
            int range = minVersionIndex + 1 - maxVersionIndex;
            return SORTED_VERSIONS.get(maxVersionIndex + random.nextInt(range));
        }
    }

    /**
     * Return consistent index settings for the provided index version.
     */
    public static ImmutableSettings.Builder settings(Version version) {
        ImmutableSettings.Builder builder = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version);
        if (version.before(Version.V_2_0_0)) {
            builder.put(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION, DjbHashFunction.class);
        }
        return builder;
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

    /**
     * Dump threads and their current stack trace.
     */
    private static String formatThreadStacks(Map<Thread, StackTraceElement[]> threads) {
        StringBuilder message = new StringBuilder();
        int cnt = 1;
        final Formatter f = new Formatter(message, Locale.ENGLISH);
        for (Map.Entry<Thread, StackTraceElement[]> e : threads.entrySet()) {
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

    public static String[] generateRandomStringArray(int maxArraySize, int maxStringSize) {
        return generateRandomStringArray(maxArraySize, maxStringSize, false);
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
     * Returns a random number of temporary paths.
     */
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

}
