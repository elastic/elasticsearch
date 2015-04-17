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
import com.carrotsearch.randomizedtesting.SysGlobals;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.uninverting.UninvertingReader;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.Closeable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

/**
 * The new base test class, with all the goodies
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
// LUCENE-6432
//@LuceneTestCase.SuppressReproduceLine
public abstract class ESTestCase extends LuceneTestCase {
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
    
    @Before
    public void disableQueryCache() {
        // TODO: Parent/child and other things does not work with the query cache
        IndexSearcher.setDefaultQueryCache(null);
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
    
    @After
    public void ensureNoFieldCacheUse() {
        // field cache should NEVER get loaded.
        String[] entries = UninvertingReader.getUninvertedStats();
        assertEquals("fieldcache must never be used, got=" + Arrays.toString(entries), 0, entries.length);
    }
    
    // old shit:
    
    /**
     * The number of concurrent JVMs used to run the tests, Default is <tt>1</tt>
     */
    public static final int CHILD_JVM_COUNT = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_COUNT, "1"));
    /**
     * The child JVM ordinal of this JVM. Default is <tt>0</tt>
     */
    public static final int CHILD_JVM_ID = Integer.parseInt(System.getProperty(SysGlobals.CHILDVM_SYSPROP_JVM_ID, "0"));

    /**
     * Annotation for backwards compat tests
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @TestGroup(enabled = false, sysProperty = TESTS_BACKWARDS_COMPATIBILITY)
    public @interface Backwards {
    }

    /**
     * Key used to set the path for the elasticsearch executable used to run backwards compatibility tests from
     * via the commandline -D{@value #TESTS_BACKWARDS_COMPATIBILITY}
     */
    public static final String TESTS_BACKWARDS_COMPATIBILITY = "tests.bwc";

    public static final String TESTS_BACKWARDS_COMPATIBILITY_VERSION = "tests.bwc.version";

    /**
     * Key used to set the path for the elasticsearch executable used to run backwards compatibility tests from
     * via the commandline -D{@value #TESTS_BACKWARDS_COMPATIBILITY_PATH}
     */
    public static final String TESTS_BACKWARDS_COMPATIBILITY_PATH = "tests.bwc.path";

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

    public static final String SYSPROP_PROCESSORS = "tests.processors";

    // -----------------------------------------------------------------
    // Truly immutable fields and constants, initialized once and valid 
    // for all suites ever since.
    // -----------------------------------------------------------------

    public static final int TESTS_PROCESSORS;

    static {
        String processors = System.getProperty(SYSPROP_PROCESSORS, ""); // mvn sets "" as default
        if (processors == null || processors.isEmpty()) {
            processors = Integer.toString(EsExecutors.boundedNumberOfProcessors(ImmutableSettings.EMPTY));
        }
        TESTS_PROCESSORS = Integer.parseInt(processors);
    }


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

    /**
     * Registers a {@link Closeable} resource that should be closed after the test
     * completes.
     *
     * @return <code>resource</code> (for call chaining).
     */
    @Override
    public <T extends Closeable> T closeAfterTest(T resource) {
        return RandomizedContext.current().closeAtEnd(resource, LifecycleScope.TEST);
    }

    /**
     * Registers a {@link Closeable} resource that should be closed after the suite
     * completes.
     *
     * @return <code>resource</code> (for call chaining).
     */
    public static <T extends Closeable> T closeAfterSuite(T resource) {
        return RandomizedContext.current().closeAtEnd(resource, LifecycleScope.SUITE);
    }
    
    // old helper stuff, a lot of it is bad news and we should see if its all used
    
    /**
     * Returns a "scaled" random number between min and max (inclusive). The number of 
     * iterations will fall between [min, max], but the selection will also try to 
     * achieve the points below: 
     * <ul>
     *   <li>the multiplier can be used to move the number of iterations closer to min
     *   (if it is smaller than 1) or closer to max (if it is larger than 1). Setting
     *   the multiplier to 0 will always result in picking min.</li>
     *   <li>on normal runs, the number will be closer to min than to max.</li>
     *   <li>on nightly runs, the number will be closer to max than to min.</li>
     * </ul>
     * 
     * @see #multiplier()
     * 
     * @param min Minimum (inclusive).
     * @param max Maximum (inclusive).
     * @return Returns a random number between min and max.
     */
    public static int scaledRandomIntBetween(int min, int max) {
        if (min < 0) throw new IllegalArgumentException("min must be >= 0: " + min);
        if (min > max) throw new IllegalArgumentException("max must be >= min: " + min + ", " + max);

        double point = Math.min(1, Math.abs(random().nextGaussian()) * 0.3) * RANDOM_MULTIPLIER;
        double range = max - min;
        int scaled = (int) Math.round(Math.min(point * range, range));
        if (isNightly()) {
          return max - scaled;
        } else {
          return min + scaled; 
        }
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
    public static byte    randomByte()     { return (byte) getRandom().nextInt(); }
    public static short   randomShort()    { return (short) getRandom().nextInt(); }
    public static int     randomInt()      { return getRandom().nextInt(); }
    public static float   randomFloat()    { return getRandom().nextFloat(); }
    public static double  randomDouble()   { return getRandom().nextDouble(); }
    public static long    randomLong()     { return getRandom().nextLong(); }
    
    /**
     * Making {@link Assume#assumeNotNull(Object...)} directly available.
     */
    public static void assumeNotNull(Object... objects) {
        Assume.assumeNotNull(objects);
    }
    
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
        return RandomInts.randomInt(getRandom(), max); 
    }

    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomAsciiOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
      return RandomStrings.randomAsciiOfLengthBetween(getRandom(), minCodeUnits,
          maxCodeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomAsciiOfLength(int codeUnits) {
      return RandomStrings.randomAsciiOfLength(getRandom(), codeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
      return RandomStrings.randomUnicodeOfLengthBetween(getRandom(),
          minCodeUnits, maxCodeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomUnicodeOfLength(int codeUnits) {
      return RandomStrings.randomUnicodeOfLength(getRandom(), codeUnits);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomUnicodeOfCodepointLengthBetween(int minCodePoints, int maxCodePoints) {
      return RandomStrings.randomUnicodeOfCodepointLengthBetween(getRandom(),
          minCodePoints, maxCodePoints);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomUnicodeOfCodepointLength(int codePoints) {
      return RandomStrings
          .randomUnicodeOfCodepointLength(getRandom(), codePoints);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
      return RandomStrings.randomRealisticUnicodeOfLengthBetween(getRandom(),
          minCodeUnits, maxCodeUnits);
    }
    
    /** @see StringGenerator#ofCodeUnitsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfLength(int codeUnits) {
      return RandomStrings.randomRealisticUnicodeOfLength(getRandom(), codeUnits);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfCodepointLengthBetween(
        int minCodePoints, int maxCodePoints) {
      return RandomStrings.randomRealisticUnicodeOfCodepointLengthBetween(
          getRandom(), minCodePoints, maxCodePoints);
    }
    
    /** @see StringGenerator#ofCodePointsLength(Random, int, int) */
    public static String randomRealisticUnicodeOfCodepointLength(int codePoints) {
      return RandomStrings.randomRealisticUnicodeOfCodepointLength(getRandom(),
          codePoints);
    }
    
    /** 
     * Return a random TimeZone from the available timezones on the system.
     * 
     * <p>Warning: This test assumes the returned array of time zones is repeatable from jvm execution
     * to jvm execution. It _may_ be different from jvm to jvm and as such, it can render
     * tests execute in a different way.</p>
     */
    public static TimeZone randomTimeZone() {
      final String[] availableIDs = TimeZone.getAvailableIDs();
      Arrays.sort(availableIDs);
      return TimeZone.getTimeZone(randomFrom(availableIDs));
    }
    
    /**
     * Shortcut for {@link RandomizedContext#current()}. 
     */
    public static RandomizedContext getContext() {
      return RandomizedContext.current();
    }
    
    /**
     * Returns true if we're running nightly tests.
     * @see Nightly
     */
    public static boolean isNightly() {
      return getContext().isNightly();
    }
    
    /** 
     * Returns a non-negative random value smaller or equal <code>max</code>. The value
     * picked is affected by {@link #isNightly()} and {@link #multiplier()}.
     * 
     * <p>This method is effectively an alias to:
     * <pre>
     * scaledRandomIntBetween(0, max)
     * </pre>
     * 
     * @see #scaledRandomIntBetween(int, int)
     */
    public static int atMost(int max) {
        if (max < 0) throw new IllegalArgumentException("atMost requires non-negative argument: " + max);
        return scaledRandomIntBetween(0, max);
    }
    
    /**
     * Making {@link Assume#assumeTrue(boolean)} directly available.
     */
    public void assumeTrue(boolean condition) {
        assumeTrue("caller was too lazy to provide a reason", condition);
    }
}
