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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope.Scope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.carrotsearch.randomizedtesting.rules.TestRuleAdapter;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.TestRuleMarkFailure;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.Version;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.MockTcpTransportPlugin;
import org.elasticsearch.transport.nio.MockNioTransportPlugin;
import org.joda.time.DateTimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

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
public abstract class ESTestCase extends LuceneTestCase {

    private static final AtomicInteger portGenerator = new AtomicInteger();

    @AfterClass
    public static void resetPortCounter() {
        portGenerator.set(0);
    }

    static {
        System.setProperty("log4j.shutdownHookEnabled", "false");
        System.setProperty("log4j2.disable.jmx", "true");

        // shutdown hook so that when the test JVM exits, logging is shutdown too
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configurator.shutdown(context);
        }));

        BootstrapForTesting.ensureInitialized();
    }

    protected final Logger logger = Loggers.getLogger(getClass());
    protected final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    private ThreadContext threadContext;

    // -----------------------------------------------------------------
    // Suite and test case setup/cleanup.
    // -----------------------------------------------------------------

    @Rule
    public RuleChain failureAndSuccessEvents = RuleChain.outerRule(new TestRuleAdapter() {
        @Override
        protected void afterIfSuccessful() throws Throwable {
            ESTestCase.this.afterIfSuccessful();
        }

        @Override
        protected void afterAlways(List<Throwable> errors) throws Throwable {
            if (errors != null && errors.isEmpty() == false) {
                boolean allAssumption = true;
                for (Throwable error : errors) {
                    if (false == error instanceof AssumptionViolatedException) {
                        allAssumption = false;
                        break;
                    }
                }
                if (false == allAssumption) {
                    ESTestCase.this.afterIfFailed(errors);
                }
            }
            super.afterAlways(errors);
        }
    });

    /**
     * Generates a new transport address using {@link TransportAddress#META_ADDRESS} with an incrementing port number.
     * The port number starts at 0 and is reset after each test suite run.
     */
    public static TransportAddress buildNewFakeTransportAddress() {
        return new TransportAddress(TransportAddress.META_ADDRESS, portGenerator.incrementAndGet());
    }

    /**
     * Called when a test fails, supplying the errors it generated. Not called when the test fails because assumptions are violated.
     */
    protected void afterIfFailed(List<Throwable> errors) {
    }

    /** called after a test is finished, but only if successful */
    protected void afterIfSuccessful() throws Exception {
    }

    // setup mock filesystems for this test run. we change PathUtils
    // so that all accesses are plumbed thru any mock wrappers

    @BeforeClass
    public static void setFileSystem() throws Exception {
        PathUtilsForTesting.setup();
    }

    @AfterClass
    public static void restoreFileSystem() throws Exception {
        PathUtilsForTesting.teardown();
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

    @Before
    public final void before()  {
        logger.info("[{}]: before test", getTestName());
        assertNull("Thread context initialized twice", threadContext);
        if (enableWarningsCheck()) {
            this.threadContext = new ThreadContext(Settings.EMPTY);
            DeprecationLogger.setThreadContext(threadContext);
        }
    }

    /**
     * Whether or not we check after each test whether it has left warnings behind. That happens if any deprecated feature or syntax
     * was used by the test and the test didn't assert on it using {@link #assertWarnings(String...)}.
     */
    protected boolean enableWarningsCheck() {
        return true;
    }

    @After
    public final void after() throws Exception {
        checkStaticState(false);
        // We check threadContext != null rather than enableWarningsCheck()
        // because after methods are still called in the event that before
        // methods failed, in which case threadContext might not have been
        // initialized
        if (threadContext != null) {
            ensureNoWarnings();
            assert threadContext == null;
        }
        ensureAllSearchContextsReleased();
        ensureCheckIndexPassed();
        logger.info("[{}]: after test", getTestName());
    }

    private void ensureNoWarnings() throws IOException {
        //Check that there are no unaccounted warning headers. These should be checked with {@link #assertWarnings(String...)} in the
        //appropriate test
        try {
            final List<String> warnings = threadContext.getResponseHeaders().get("Warning");
            assertNull("unexpected warning headers", warnings);
        } finally {
            resetDeprecationLogger(false);
        }
    }

    /**
     * Convenience method to assert warnings for settings deprecations and general deprecation warnings.
     *
     * @param settings the settings that are expected to be deprecated
     * @param warnings other expected general deprecation warnings
     */
    protected final void assertSettingDeprecationsAndWarnings(final Setting<?>[] settings, final String... warnings) {
        assertWarnings(
                Stream.concat(
                        Arrays
                                .stream(settings)
                                .map(Setting::getKey)
                                .map(k -> "[" + k + "] setting was deprecated in Elasticsearch and will be removed in a future release! " +
                                        "See the breaking changes documentation for the next major version."),
                        Arrays.stream(warnings))
                        .toArray(String[]::new));
    }

    protected final void assertWarnings(String... expectedWarnings) {
        if (enableWarningsCheck() == false) {
            throw new IllegalStateException("unable to check warning headers if the test is not set to do so");
        }
        try {
            final List<String> actualWarnings = threadContext.getResponseHeaders().get("Warning");
            assertNotNull(actualWarnings);
            final Set<String> actualWarningValues =
                    actualWarnings.stream().map(DeprecationLogger::extractWarningValueFromWarningHeader).collect(Collectors.toSet());
            for (String msg : expectedWarnings) {
                assertThat(actualWarningValues, hasItem(DeprecationLogger.escapeAndEncode(msg)));
            }
            assertEquals("Expected " + expectedWarnings.length + " warnings but found " + actualWarnings.size() + "\nExpected: "
                    + Arrays.asList(expectedWarnings) + "\nActual: " + actualWarnings,
                expectedWarnings.length, actualWarnings.size());
        } finally {
            resetDeprecationLogger(true);
        }
    }

    /**
     * Reset the deprecation logger by removing the current thread context, and setting a new thread context if {@code setNewThreadContext}
     * is set to {@code true} and otherwise clearing the current thread context.
     *
     * @param setNewThreadContext whether or not to attach a new thread context to the deprecation logger
     */
    private void resetDeprecationLogger(final boolean setNewThreadContext) {
        // "clear" current warning headers by setting a new ThreadContext
        DeprecationLogger.removeThreadContext(this.threadContext);
        try {
            this.threadContext.close();
            // catch IOException to avoid that call sites have to deal with it. It is only declared because this class implements Closeable
            // but it is impossible that this implementation will ever throw an IOException.
        } catch (IOException ex) {
            throw new AssertionError("IOException thrown while closing deprecation logger's thread context", ex);
        }
        if (setNewThreadContext) {
            this.threadContext = new ThreadContext(Settings.EMPTY);
            DeprecationLogger.setThreadContext(this.threadContext);
        } else {
            this.threadContext = null;
        }
    }

    private static final List<StatusData> statusData = new ArrayList<>();
    static {
        // ensure that the status logger is set to the warn level so we do not miss any warnings with our Log4j usage
        StatusLogger.getLogger().setLevel(Level.WARN);
        // Log4j will write out status messages indicating problems with the Log4j usage to the status logger; we hook into this logger and
        // assert that no such messages were written out as these would indicate a problem with our logging configuration
        StatusLogger.getLogger().registerListener(new StatusConsoleListener(Level.WARN) {

            @Override
            public void log(StatusData data) {
                synchronized (statusData) {
                    statusData.add(data);
                }
            }

        });
    }

    // separate method so that this can be checked again after suite scoped cluster is shut down
    protected static void checkStaticState(boolean afterClass) throws Exception {
        if (afterClass) {
            MockPageCacheRecycler.ensureAllPagesAreReleased();
        }
        MockBigArrays.ensureAllArraysAreReleased();

        // ensure no one changed the status logger level on us
        assertThat(StatusLogger.getLogger().getLevel(), equalTo(Level.WARN));
        synchronized (statusData) {
            try {
                // ensure that there are no status logger messages which would indicate a problem with our Log4j usage; we map the
                // StatusData instances to Strings as otherwise their toString output is useless
                assertThat(
                    statusData.stream().map(status -> status.getMessage().getFormattedMessage()).collect(Collectors.toList()),
                    empty());
            } finally {
                // we clear the list so that status data from other tests do not interfere with tests within the same JVM
                statusData.clear();
            }
        }
    }

    // this must be a separate method from other ensure checks above so suite scoped integ tests can call...TODO: fix that
    public final void ensureAllSearchContextsReleased() throws Exception {
        assertBusy(() -> MockSearchService.assertNoInFlightContext());
    }

    // mockdirectorywrappers currently set this boolean if checkindex fails
    // TODO: can we do this cleaner???

    /** MockFSDirectoryService sets this: */
    public static boolean checkIndexFailed;

    @Before
    public final void resetCheckIndexStatus() throws Exception {
        checkIndexFailed = false;
    }

    public final void ensureCheckIndexPassed() throws Exception {
        assertFalse("at least one shard failed CheckIndex", checkIndexFailed);
    }

    // -----------------------------------------------------------------
    // Test facilities and facades for subclasses.
    // -----------------------------------------------------------------

    // TODO: decide on one set of naming for between/scaledBetween and remove others
    // TODO: replace frequently() with usually()

    /**
     * Returns a "scaled" random number between min and max (inclusive).
     *
     * @see RandomizedTest#scaledRandomIntBetween(int, int)
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
        return RandomNumbers.randomIntBetween(random(), min, max);
    }

    /**
     * A random long number between min (inclusive) and max (inclusive).
     */
    public static long randomLongBetween(long min, long max) {
        return RandomNumbers.randomLongBetween(random(), min, max);
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

    /**
     * @return a <code>long</code> between <code>0</code> and <code>Long.MAX_VALUE</code> (inclusive) chosen uniformly at random.
     */
    public static long randomNonNegativeLong() {
        long randomLong = randomLong();
        return randomLong == Long.MIN_VALUE ? 0 : Math.abs(randomLong);
    }

    public static float randomFloat() {
        return random().nextFloat();
    }

    public static double randomDouble() {
        return random().nextDouble();
    }

    /**
     * Returns a double value in the interval [start, end) if lowerInclusive is
     * set to true, (start, end) otherwise.
     *
     * @param start          lower bound of interval to draw uniformly distributed random numbers from
     * @param end            upper bound
     * @param lowerInclusive whether or not to include lower end of the interval
     */
    public static double randomDoubleBetween(double start, double end, boolean lowerInclusive) {
        double result = 0.0;

        if (start == -Double.MAX_VALUE || end == Double.MAX_VALUE) {
            // formula below does not work with very large doubles
            result = Double.longBitsToDouble(randomLong());
            while (result < start || result > end || Double.isNaN(result)) {
                result = Double.longBitsToDouble(randomLong());
            }
        } else {
            result = randomDouble();
            if (lowerInclusive == false) {
                while (result <= 0.0) {
                    result = randomDouble();
                }
            }
            result = result * end + (1.0 - result) * start;
        }
        return result;
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
        return randomFrom(random(), array);
    }

    /** Pick a random object from the given array. The array must not be empty. */
    public static <T> T randomFrom(Random random, T... array) {
        return RandomPicks.randomFrom(random, array);
    }

    /** Pick a random object from the given list. */
    public static <T> T randomFrom(List<T> list) {
        return RandomPicks.randomFrom(random(), list);
    }

    /** Pick a random object from the given collection. */
    public static <T> T randomFrom(Collection<T> collection) {
        return randomFrom(random(), collection);
    }

    /** Pick a random object from the given collection. */
    public static <T> T randomFrom(Random random, Collection<T> collection) {
        return RandomPicks.randomFrom(random, collection);
    }

    public static String randomAlphaOfLengthBetween(int minCodeUnits, int maxCodeUnits) {
        return RandomizedTest.randomAsciiOfLengthBetween(minCodeUnits, maxCodeUnits);
    }

    public static String randomAlphaOfLength(int codeUnits) {
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

    public static String[] generateRandomStringArray(int maxArraySize, int maxStringSize, boolean allowNull, boolean allowEmpty) {
        if (allowNull && random().nextBoolean()) {
            return null;
        }
        int arraySize = randomIntBetween(allowEmpty ? 0 : 1, maxArraySize);
        String[] array = new String[arraySize];
        for (int i = 0; i < arraySize; i++) {
            array[i] = RandomStrings.randomAsciiOfLength(random(), maxStringSize);
        }
        return array;
    }

    public static String[] generateRandomStringArray(int maxArraySize, int maxStringSize, boolean allowNull) {
        return generateRandomStringArray(maxArraySize, maxStringSize, allowNull, true);
    }

    private static final String[] TIME_SUFFIXES = new String[]{"d", "h", "ms", "s", "m", "micros", "nanos"};

    public static String randomTimeValue(int lower, int upper, String... suffixes) {
        return randomIntBetween(lower, upper) + randomFrom(suffixes);
    }

    public static String randomTimeValue(int lower, int upper) {
        return randomTimeValue(lower, upper, TIME_SUFFIXES);
    }

    public static String randomTimeValue() {
        return randomTimeValue(0, 1000);
    }

    public static String randomPositiveTimeValue() {
        return randomTimeValue(1, 1000);
    }

    /**
     * generate a random DateTimeZone from the ones available in joda library
     */
    public static DateTimeZone randomDateTimeZone() {
        List<String> ids = new ArrayList<>(DateTimeZone.getAvailableIDs());
        Collections.sort(ids);
        return DateTimeZone.forID(randomFrom(ids));
    }

    /**
     * generate a random ZoneId from the ones available in java time
     */
    public static ZoneId randomZoneId() {
        List<String> ids = new ArrayList<>(ZoneId.getAvailableZoneIds());
        Collections.sort(ids);
        return ZoneId.of(randomFrom(ids));
    }

    /**
     * helper to randomly perform on <code>consumer</code> with <code>value</code>
     */
    public static <T> void maybeSet(Consumer<T> consumer, T value) {
        if (randomBoolean()) {
            consumer.accept(value);
        }
    }

    /**
     * helper to get a random value in a certain range that's different from the input
     */
    public static <T> T randomValueOtherThan(T input, Supplier<T> randomSupplier) {
        return randomValueOtherThanMany(v -> Objects.equals(input, v), randomSupplier);
    }

    /**
     * helper to get a random value in a certain range that's different from the input
     */
    public static <T> T randomValueOtherThanMany(Predicate<T> input, Supplier<T> randomSupplier) {
        T randomValue = null;
        do {
            randomValue = randomSupplier.get();
        } while (input.test(randomValue));
        return randomValue;
    }

    /**
     * Runs the code block for 10 seconds waiting for no assertion to trip.
     */
    public static void assertBusy(CheckedRunnable<Exception> codeBlock) throws Exception {
        assertBusy(codeBlock, 10, TimeUnit.SECONDS);
    }

    /**
     * Runs the code block for the provided interval, waiting for no assertions to trip.
     */
    public static void assertBusy(CheckedRunnable<Exception> codeBlock, long maxWaitTime, TimeUnit unit) throws Exception {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long iterations = Math.max(Math.round(Math.log10(maxTimeInMillis) / Math.log10(2)), 1);
        long timeInMillis = 1;
        long sum = 0;
        List<AssertionError> failures = new ArrayList<>();
        for (int i = 0; i < iterations; i++) {
            try {
                codeBlock.run();
                return;
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
            codeBlock.run();
        } catch (AssertionError e) {
            for (AssertionError failure : failures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
    }

    public static boolean awaitBusy(BooleanSupplier breakSupplier) throws InterruptedException {
        return awaitBusy(breakSupplier, 10, TimeUnit.SECONDS);
    }

    // After 1s, we stop growing the sleep interval exponentially and just sleep 1s until maxWaitTime
    private static final long AWAIT_BUSY_THRESHOLD = 1000L;

    public static boolean awaitBusy(BooleanSupplier breakSupplier, long maxWaitTime, TimeUnit unit) throws InterruptedException {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long timeInMillis = 1;
        long sum = 0;
        while (sum + timeInMillis < maxTimeInMillis) {
            if (breakSupplier.getAsBoolean()) {
                return true;
            }
            Thread.sleep(timeInMillis);
            sum += timeInMillis;
            timeInMillis = Math.min(AWAIT_BUSY_THRESHOLD, timeInMillis * 2);
        }
        timeInMillis = maxTimeInMillis - sum;
        Thread.sleep(Math.max(timeInMillis, 0));
        return breakSupplier.getAsBoolean();
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

    public static boolean terminate(ThreadPool threadPool) throws InterruptedException {
        return ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
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

    public Path getBwcIndicesPath() {
        return getDataPath("/indices/bwc");
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
        return newNodeEnvironment(Settings.EMPTY);
    }

    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = Settings.builder()
                .put(settings)
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths()).build();
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    /** Return consistent index settings for the provided index version. */
    public static Settings.Builder settings(Version version) {
        Settings.Builder builder = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version);
        return builder;
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

    /**
     * Returns size random values
     */
    public static <T> List<T> randomSubsetOf(int size, T... values) {
        List<T> list = arrayAsArrayList(values);
        return randomSubsetOf(size, list);
    }

    /**
     * Returns a random subset of values (including a potential empty list)
     */
    public static <T> List<T> randomSubsetOf(Collection<T> collection) {
        return randomSubsetOf(randomInt(Math.max(collection.size() - 1, 0)), collection);
    }

    /**
     * Returns size random values
     */
    public static <T> List<T> randomSubsetOf(int size, Collection<T> collection) {
        if (size > collection.size()) {
            throw new IllegalArgumentException("Can\'t pick " + size + " random objects from a collection of " +
                    collection.size() + " objects");
        }
        List<T> tempList = new ArrayList<>(collection);
        Collections.shuffle(tempList, random());
        return tempList.subList(0, size);
    }

    /**
     * Builds a set of unique items. Usually you'll get the requested count but you might get less than that number if the supplier returns
     * lots of repeats. Make sure that the items properly implement equals and hashcode.
     */
    public static <T> Set<T> randomUnique(Supplier<T> supplier, int targetCount) {
        Set<T> things = new HashSet<>();
        int maxTries = targetCount * 10;
        for (int t = 0; t < maxTries; t++) {
            if (things.size() == targetCount) {
                return things;
            }
            things.add(supplier.get());
        }
        // Oh well, we didn't get enough unique things. It'll be ok.
        return things;
    }

    public static String randomGeohash(int minPrecision, int maxPrecision) {
        return geohashGenerator.ofStringLength(random(), minPrecision, maxPrecision);
    }

    private static boolean useNio;

    @BeforeClass
    public static void setUseNio() throws Exception {
        useNio = randomBoolean();
    }

    public static String getTestTransportType() {
        return useNio ? MockNioTransportPlugin.MOCK_NIO_TRANSPORT_NAME : MockTcpTransportPlugin.MOCK_TCP_TRANSPORT_NAME;
    }

    public static Class<? extends Plugin> getTestTransportPlugin() {
        return useNio ? MockNioTransportPlugin.class : MockTcpTransportPlugin.class;
    }

    private static final GeohashGenerator geohashGenerator = new GeohashGenerator();

    public static class GeohashGenerator extends CodepointSetGenerator {
        private static final char[] ASCII_SET = "0123456789bcdefghjkmnpqrstuvwxyz".toCharArray();

        public GeohashGenerator() {
            super(ASCII_SET);
        }
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns. Shuffles the keys to make sure that parsing never relies on keys ordering.
     */
    protected final BytesReference toShuffledXContent(ToXContent toXContent, XContentType xContentType, ToXContent.Params params,
                                                      boolean humanReadable, String... exceptFieldNames) throws IOException{
        BytesReference bytes = XContentHelper.toXContent(toXContent, xContentType, params, humanReadable);
        try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
            try (XContentBuilder builder = shuffleXContent(parser, rarely(), exceptFieldNames)) {
                return builder.bytes();
            }
        }
    }

    /**
     * Randomly shuffles the fields inside objects in the {@link XContentBuilder} passed in.
     * Recursively goes through inner objects and also shuffles them. Exceptions for this
     * recursive shuffling behavior can be made by passing in the names of fields which
     * internally should stay untouched.
     */
    protected final XContentBuilder shuffleXContent(XContentBuilder builder, String... exceptFieldNames) throws IOException {
        try (XContentParser parser = createParser(builder)) {
            return shuffleXContent(parser, builder.isPrettyPrint(), exceptFieldNames);
        }
    }

    /**
     * Randomly shuffles the fields inside objects parsed using the {@link XContentParser} passed in.
     * Recursively goes through inner objects and also shuffles them. Exceptions for this
     * recursive shuffling behavior can be made by passing in the names of fields which
     * internally should stay untouched.
     */
    public XContentBuilder shuffleXContent(XContentParser parser, boolean prettyPrint, String... exceptFieldNames) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType());
        if (prettyPrint) {
            xContentBuilder.prettyPrint();
        }
        Token token = parser.currentToken() == null ? parser.nextToken() : parser.currentToken();
        if (token == Token.START_ARRAY) {
            List<Object> shuffledList = shuffleList(parser.listOrderedMap(), new HashSet<>(Arrays.asList(exceptFieldNames)));
            return xContentBuilder.value(shuffledList);
        }
        //we need a sorted map for reproducibility, as we are going to shuffle its keys and write XContent back
        Map<String, Object> shuffledMap = shuffleMap((LinkedHashMap<String, Object>)parser.mapOrdered(),
            new HashSet<>(Arrays.asList(exceptFieldNames)));
        return xContentBuilder.map(shuffledMap);
    }

    // shuffle fields of objects in the list, but not the list itself
    private static List<Object> shuffleList(List<Object> list, Set<String> exceptFields) {
        List<Object> targetList = new ArrayList<>();
        for(Object value : list) {
            if (value instanceof Map) {
                @SuppressWarnings("unchecked")
                LinkedHashMap<String, Object> valueMap = (LinkedHashMap<String, Object>) value;
                targetList.add(shuffleMap(valueMap, exceptFields));
            } else if(value instanceof List) {
                targetList.add(shuffleList((List) value, exceptFields));
            }  else {
                targetList.add(value);
            }
        }
        return targetList;
    }

    public static LinkedHashMap<String, Object> shuffleMap(LinkedHashMap<String, Object> map, Set<String> exceptFields) {
        List<String> keys = new ArrayList<>(map.keySet());
        LinkedHashMap<String, Object> targetMap = new LinkedHashMap<>();
        Collections.shuffle(keys, random());
        for (String key : keys) {
            Object value = map.get(key);
            if (value instanceof Map && exceptFields.contains(key) == false) {
                @SuppressWarnings("unchecked")
                LinkedHashMap<String, Object> valueMap = (LinkedHashMap<String, Object>) value;
                targetMap.put(key, shuffleMap(valueMap, exceptFields));
            } else if(value instanceof List && exceptFields.contains(key) == false) {
                targetMap.put(key, shuffleList((List) value, exceptFields));
            } else {
                targetMap.put(key, value);
            }
        }
        return targetMap;
    }

    /**
     * Create a copy of an original {@link Writeable} object by running it through a {@link BytesStreamOutput} and
     * reading it in again using a provided {@link Writeable.Reader}. The stream that is wrapped around the {@link StreamInput}
     * potentially need to use a {@link NamedWriteableRegistry}, so this needs to be provided too (although it can be
     * empty if the object that is streamed doesn't contain any {@link NamedWriteable} objects itself.
     */
    public static <T extends Writeable> T copyWriteable(T original, NamedWriteableRegistry namedWritabelRegistry,
            Writeable.Reader<T> reader) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWritabelRegistry)) {
                return reader.read(in);
            }
        }
    }

    public void assertAllIndicesRemovedAndDeletionCompleted(Iterable<IndicesService> indicesServices) throws Exception {
        for (IndicesService indicesService : indicesServices) {
            assertBusy(() -> assertFalse(indicesService.iterator().hasNext()), 1, TimeUnit.MINUTES);
            assertBusy(() -> assertFalse(indicesService.hasUncompletedPendingDeletes()), 1, TimeUnit.MINUTES);
        }
    }

    /**
     * Asserts that there are no files in the specified path
     */
    public void assertPathHasBeenCleared(Path path) {
        logger.info("--> checking that [{}] has been cleared", path);
        int count = 0;
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        if (Files.exists(path)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(path)) {
                for (Path file : stream) {
                    // Skip files added by Lucene's ExtraFS
                    if (file.getFileName().toString().startsWith("extra")) {
                        continue;
                    }
                    logger.info("--> found file: [{}]", file.toAbsolutePath().toString());
                    if (Files.isDirectory(file)) {
                        assertPathHasBeenCleared(file);
                    } else if (Files.isRegularFile(file)) {
                        count++;
                        sb.append(file.toAbsolutePath().toString());
                        sb.append("\n");
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        sb.append("]");
        assertThat(count + " files exist that should have been cleaned:\n" + sb.toString(), count, equalTo(0));
    }

    /**
     * Assert that two objects are equals, calling {@link ToXContent#toXContent(XContentBuilder, ToXContent.Params)} to print out their
     * differences if they aren't equal.
     */
    public static <T extends ToXContent> void assertEqualsWithErrorMessageFromXContent(T expected, T actual) {
        if (Objects.equals(expected, actual)) {
            return;
        }
        if (expected == null) {
            throw new AssertionError("Expected null be actual was [" + actual.toString() + "]");
        }
        if (actual == null) {
            throw new AssertionError("Didn't expect null but actual was [null]");
        }
        try (XContentBuilder actualJson = JsonXContent.contentBuilder();
                XContentBuilder expectedJson = JsonXContent.contentBuilder()) {
            actualJson.startObject();
            actual.toXContent(actualJson, ToXContent.EMPTY_PARAMS);
            actualJson.endObject();
            expectedJson.startObject();
            expected.toXContent(expectedJson, ToXContent.EMPTY_PARAMS);
            expectedJson.endObject();
            NotEqualMessageBuilder message = new NotEqualMessageBuilder();
            message.compareMaps(
                    XContentHelper.convertToMap(actualJson.bytes(), false).v2(),
                    XContentHelper.convertToMap(expectedJson.bytes(), false).v2());
            throw new AssertionError("Didn't match expected value:\n" + message);
        } catch (IOException e) {
            throw new AssertionError("IOException while building failure message", e);
        }
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContentBuilder builder) throws IOException {
        return builder.generator().contentType().xContent().createParser(xContentRegistry(), builder.bytes());
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, String data) throws IOException {
        return xContent.createParser(xContentRegistry(), data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        return xContent.createParser(xContentRegistry(), data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, byte[] data) throws IOException {
        return xContent.createParser(xContentRegistry(), data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
        return xContent.createParser(xContentRegistry(), data);
    }

    /**
     * The {@link NamedXContentRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
    }

    /**
     * The {@link NamedWriteableRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
    }

    /**
     * Create a "mock" script for use either with {@link MockScriptEngine} or anywhere where you need a script but don't really care about
     * its contents.
     */
    public static final Script mockScript(String id) {
        return new Script(ScriptType.INLINE, MockScriptEngine.NAME, id, emptyMap());
    }

    /** Returns the suite failure marker: internal use only! */
    public static TestRuleMarkFailure getSuiteFailureMarker() {
        return suiteFailureMarker;
    }

    /** Compares two stack traces, ignoring module (which is not yet serialized) */
    public static void assertArrayEquals(StackTraceElement expected[], StackTraceElement actual[]) {
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i]);
        }
    }

    /** Compares two stack trace elements, ignoring module (which is not yet serialized) */
    public static void assertEquals(StackTraceElement expected, StackTraceElement actual) {
        assertEquals(expected.getClassName(), actual.getClassName());
        assertEquals(expected.getMethodName(), actual.getMethodName());
        assertEquals(expected.getFileName(), actual.getFileName());
        assertEquals(expected.getLineNumber(), actual.getLineNumber());
        assertEquals(expected.isNativeMethod(), actual.isNativeMethod());
    }

    protected static long spinForAtLeastOneMillisecond() {
        return spinForAtLeastNMilliseconds(1);
    }

    protected static long spinForAtLeastNMilliseconds(final long ms) {
        long nanosecondsInMillisecond = TimeUnit.NANOSECONDS.convert(ms, TimeUnit.MILLISECONDS);
        /*
         * Force at least ms milliseconds to elapse, but ensure the clock has enough resolution to
         * observe the passage of time.
         */
        long start = System.nanoTime();
        long elapsed;
        while ((elapsed = (System.nanoTime() - start)) < nanosecondsInMillisecond) {
            // busy spin
        }
        return elapsed;
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(Index index, Settings settings, AnalysisPlugin... analysisPlugins)
            throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        return createTestAnalysis(index, nodeSettings, settings, analysisPlugins);
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(Index index, Settings nodeSettings, Settings settings,
                                                  AnalysisPlugin... analysisPlugins) throws IOException {
        Settings indexSettings = Settings.builder().put(settings)
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .build();
        return createTestAnalysis(IndexSettingsModule.newIndexSettings(index, indexSettings), nodeSettings, analysisPlugins);
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(IndexSettings indexSettings, Settings nodeSettings,
                                                  AnalysisPlugin... analysisPlugins) throws IOException {
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        AnalysisModule analysisModule = new AnalysisModule(env, Arrays.asList(analysisPlugins));
        AnalysisRegistry analysisRegistry = analysisModule.getAnalysisRegistry();
        return new TestAnalysis(analysisRegistry.build(indexSettings),
            analysisRegistry.buildTokenFilterFactories(indexSettings),
            analysisRegistry.buildTokenizerFactories(indexSettings),
            analysisRegistry.buildCharFilterFactories(indexSettings));
    }

    public static ScriptModule newTestScriptModule() {
        return new ScriptModule(Settings.EMPTY, singletonList(new ScriptPlugin() {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap("1", script -> "1"));
            }
        }));
    }

    /** Creates an IndicesModule for testing with the given mappers and metadata mappers. */
    public static IndicesModule newTestIndicesModule(Map<String, Mapper.TypeParser> extraMappers,
                                                     Map<String, MetadataFieldMapper.TypeParser> extraMetadataMappers) {
        return new IndicesModule(Collections.singletonList(
            new MapperPlugin() {
                @Override
                public Map<String, Mapper.TypeParser> getMappers() {
                    return extraMappers;
                }
                @Override
                public Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers() {
                    return extraMetadataMappers;
                }
            }
        ));
    }

    /**
     * This cute helper class just holds all analysis building blocks that are used
     * to build IndexAnalyzers. This is only for testing since in production we only need the
     * result and we don't even expose it there.
     */
    public static final class TestAnalysis {

        public final IndexAnalyzers indexAnalyzers;
        public final Map<String, TokenFilterFactory> tokenFilter;
        public final Map<String, TokenizerFactory> tokenizer;
        public final Map<String, CharFilterFactory> charFilter;

        public TestAnalysis(IndexAnalyzers indexAnalyzers,
                            Map<String, TokenFilterFactory> tokenFilter,
                            Map<String, TokenizerFactory> tokenizer,
                            Map<String, CharFilterFactory> charFilter) {
            this.indexAnalyzers = indexAnalyzers;
            this.tokenFilter = tokenFilter;
            this.tokenizer = tokenizer;
            this.charFilter = charFilter;
        }
    }

}
