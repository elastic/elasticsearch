/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
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
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.status.StatusConsoleListener;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.tests.util.TestRuleMarkFailure;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.tests.util.TimeUnits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RequestBuilder;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.logging.HeaderWarningAppender;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.entitlement.bootstrap.TestEntitlementBootstrap;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService.IndexCreationContext;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.logging.internal.spi.LoggerFactory;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.transport.netty4.Netty4Plugin;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.RuleChain;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;

/**
 * Base testcase for randomized unit testing with Elasticsearch
 */
@Listeners({ ReproduceInfoPrinter.class, LoggingListener.class })
@ThreadLeakScope(Scope.SUITE)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE)
@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class, NettyGlobalThreadsFilter.class, JnaCleanerThreadsFilter.class })
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
// we suppress pretty much all the lucene codecs for now, except asserting
// assertingcodec is the winner for a codec here: it finds bugs and gives clear exceptions.
@SuppressCodecs(
    {
        "SimpleText",
        "Memory",
        "CheapBastard",
        "Direct",
        "Compressing",
        "FST50",
        "FSTOrd50",
        "TestBloomFilteredLucenePostings",
        "MockRandom",
        "BlockTreeOrds",
        "LuceneFixedGap",
        "LuceneVarGapFixedInterval",
        "LuceneVarGapDocFreqInterval",
        "Lucene50" }
)
@LuceneTestCase.SuppressReproduceLine
public abstract class ESTestCase extends LuceneTestCase {

    protected static final List<String> JAVA_TIMEZONE_IDS;
    protected static final List<String> JAVA_ZONE_IDS;

    private static final AtomicInteger portGenerator = new AtomicInteger();

    private static final Collection<String> loggedLeaks = new ArrayList<>();

    private HeaderWarningAppender headerWarningAppender;

    @AfterClass
    public static void resetPortCounter() {
        portGenerator.set(0);
    }

    // Allows distinguishing between parallel test processes
    public static final String TEST_WORKER_VM_ID;

    public static final String TEST_WORKER_SYS_PROPERTY = "org.gradle.test.worker";

    public static final String DEFAULT_TEST_WORKER_ID = "--not-gradle--";

    public static final String FIPS_SYSPROP = "tests.fips.enabled";

    private static final SetOnce<Boolean> WARN_SECURE_RANDOM_FIPS_NOT_DETERMINISTIC = new SetOnce<>();

    private static final String LOWER_ALPHA_CHARACTERS = "abcdefghijklmnopqrstuvwxyz";
    private static final String UPPER_ALPHA_CHARACTERS = LOWER_ALPHA_CHARACTERS.toUpperCase(Locale.ROOT);
    private static final String DIGIT_CHARACTERS = "0123456789";
    private static final String ALPHANUMERIC_CHARACTERS = LOWER_ALPHA_CHARACTERS + UPPER_ALPHA_CHARACTERS + DIGIT_CHARACTERS;

    static {
        Random random = initTestSeed();
        TEST_WORKER_VM_ID = System.getProperty(TEST_WORKER_SYS_PROPERTY, DEFAULT_TEST_WORKER_ID);
        setTestSysProps(random);
        // TODO: consolidate logging initialization for tests so it all occurs in logconfigurator
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
        MockLog.init();

        final List<Appender> testAppenders = new ArrayList<>(3);
        for (String leakLoggerName : Arrays.asList("io.netty.util.ResourceLeakDetector", LeakTracker.class.getName())) {
            Logger leakLogger = LogManager.getLogger(leakLoggerName);
            Appender leakAppender = new AbstractAppender(leakLoggerName, null, PatternLayout.newBuilder().withPattern("%m").build()) {
                @Override
                public void append(LogEvent event) {
                    String message = event.getMessage().getFormattedMessage();
                    if (Level.ERROR.equals(event.getLevel()) && message.contains("LEAK:")) {
                        synchronized (loggedLeaks) {
                            loggedLeaks.add(message);
                        }
                    }
                }
            };
            leakAppender.start();
            Loggers.addAppender(leakLogger, leakAppender);
            testAppenders.add(leakAppender);
        }
        Logger promiseUncaughtLogger = LogManager.getLogger("io.netty.util.concurrent.DefaultPromise");
        final Appender uncaughtAppender = new AbstractAppender(
            promiseUncaughtLogger.getName(),
            null,
            PatternLayout.newBuilder().withPattern("%m").build()
        ) {
            @Override
            public void append(LogEvent event) {
                if (Level.WARN.equals(event.getLevel())) {
                    synchronized (loggedLeaks) {
                        loggedLeaks.add(event.getMessage().getFormattedMessage());
                    }
                }
            }
        };
        uncaughtAppender.start();
        Loggers.addAppender(promiseUncaughtLogger, uncaughtAppender);
        testAppenders.add(uncaughtAppender);
        // shutdown hook so that when the test JVM exits, logging is shutdown too
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Appender testAppender : testAppenders) {
                testAppender.stop();
            }
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configurator.shutdown(context);
        }));

        BootstrapForTesting.ensureInitialized();

        /*
         * We need to exclude time zones not supported by joda (like SystemV* timezones)
         * because they cannot be converted back to DateTimeZone which we currently
         * still need to do internally e.g. in bwc serialization and in the extract() method
         * //TODO remove once tests do not send time zone ids back to versions of ES using Joda
         */
        Set<String> unsupportedJodaTZIds = Set.of(
            "ACT",
            "AET",
            "AGT",
            "ART",
            "AST",
            "BET",
            "BST",
            "CAT",
            "CNT",
            "CST",
            "CTT",
            "EAT",
            "ECT",
            "EST",
            "HST",
            "IET",
            "IST",
            "JST",
            "MIT",
            "MST",
            "NET",
            "NST",
            "PLT",
            "PNT",
            "PRT",
            "PST",
            "SST",
            "VST"
        );
        Predicate<String> unsupportedZoneIdsPredicate = tz -> tz.startsWith("System/") || tz.equals("Eire");
        Predicate<String> unsupportedTZIdsPredicate = unsupportedJodaTZIds::contains;

        JAVA_TIMEZONE_IDS = Arrays.stream(TimeZone.getAvailableIDs())
            .filter(unsupportedTZIdsPredicate.negate())
            .filter(unsupportedZoneIdsPredicate.negate())
            .sorted()
            .toList();

        JAVA_ZONE_IDS = ZoneId.getAvailableZoneIds().stream().filter(unsupportedZoneIdsPredicate.negate()).sorted().toList();
    }

    protected static Random initTestSeed() {
        String inputSeed = System.getProperty("tests.seed");
        long seed;
        if (inputSeed == null) {
            // when running tests in intellij, we don't have a seed. Setup the seed early here, before getting to RandomizedRunner,
            // so that we can use it in ESTestCase static init
            seed = System.nanoTime();
            setTestSeed(Long.toHexString(seed));
        } else {
            String[] seedParts = inputSeed.split("[\\:]");
            seed = Long.parseUnsignedLong(seedParts[0], 16);
        }

        if (Booleans.parseBoolean(System.getProperty("tests.hackImmutableCollections", "false"))) {
            forceImmutableCollectionsSeed(seed);
        }

        return new Random(seed);
    }

    @SuppressForbidden(reason = "set tests.seed for intellij")
    static void setTestSeed(String seed) {
        System.setProperty("tests.seed", seed);
    }

    private static void forceImmutableCollectionsSeed(long seed) {
        try {
            MethodHandles.Lookup lookup = MethodHandles.lookup();
            Class<?> collectionsClass = Class.forName("java.util.ImmutableCollections");
            var salt32l = lookup.findStaticVarHandle(collectionsClass, "SALT32L", long.class);
            var reverse = lookup.findStaticVarHandle(collectionsClass, "REVERSE", boolean.class);
            salt32l.set(seed & 0xFFFF_FFFFL);
            reverse.set((seed & 1) == 0);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    @SuppressForbidden(reason = "force log4j and netty sysprops")
    private static void setTestSysProps(Random random) {
        System.setProperty("log4j.shutdownHookEnabled", "false");
        System.setProperty("log4j2.disable.jmx", "true");

        // Enable Netty leak detection and monitor logger for logged leak errors
        System.setProperty("io.netty.leakDetection.level", "paranoid");
        if (System.getProperty("es.use_unpooled_allocator") == null) {
            // unless explicitly forced to unpooled, always test with the pooled allocator to get the best possible coverage from Netty's
            // leak detection which does not cover simple unpooled heap buffers
            System.setProperty("es.use_unpooled_allocator", "false");
        }

        // We have to disable setting the number of available processors as tests in the same JVM randomize processors and will step on each
        // other if we allow them to set the number of available processors as it's set-once in Netty.
        System.setProperty("es.set.netty.runtime.available.processors", "false");
    }

    protected final Logger logger = LogManager.getLogger(getClass());
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
    protected void afterIfFailed(List<Throwable> errors) {}

    /** called after a test is finished, but only if successful */
    protected void afterIfSuccessful() throws Exception {}

    /**
     * Marks a test suite or a test method that should run without checking for entitlements.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target( ElementType.TYPE )
    public @interface WithoutEntitlements {
    }

    @BeforeClass
    public static void setupEntitlementsForClass() {
        boolean isActive = false == getTestClass().isAnnotationPresent(WithoutEntitlements.class);
        TestEntitlementBootstrap.setIsActive(isActive);
    }

    @AfterClass
    public static void enableEntitlements() {
        TestEntitlementBootstrap.restoreDefaultIsActive();
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
        Requests.INDEX_CONTENT_TYPE = randomFrom(XContentType.values());
    }

    @AfterClass
    public static void restoreContentType() {
        Requests.INDEX_CONTENT_TYPE = XContentType.JSON;
    }

    @BeforeClass
    public static void ensureSupportedLocale() {
        if (isUnusableLocale()) {
            Logger logger = LogManager.getLogger(ESTestCase.class);
            logger.warn(
                "Attempting to run tests in an unusable locale in a FIPS JVM. Certificate expiration validation will fail, "
                    + "switching to English. See: https://github.com/bcgit/bc-java/issues/405"
            );
            Locale.setDefault(Locale.ENGLISH);
        }
    }

    @Before
    public void setHeaderWarningAppender() {
        this.headerWarningAppender = HeaderWarningAppender.createAppender("header_warning", null);
        this.headerWarningAppender.start();
        Loggers.addAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.headerWarningAppender);
    }

    @After
    public void removeHeaderWarningAppender() {
        if (this.headerWarningAppender != null) {
            Loggers.removeAppender(LogManager.getLogger("org.elasticsearch.deprecation"), this.headerWarningAppender);
            this.headerWarningAppender = null;
        }
    }

    private static org.elasticsearch.logging.Level capturedLogLevel = null;

    // just capture the expected level once before the suite starts
    @BeforeClass
    public static void captureLoggingLevel() {
        capturedLogLevel = LoggerFactory.provider().getRootLevel();
    }

    @AfterClass
    public static void restoreLoggingLevel() {
        if (capturedLogLevel != null) {
            // log level might not have been captured if suite was skipped
            LoggerFactory.provider().setRootLevel(capturedLogLevel);
            capturedLogLevel = null;
        }
    }

    @Before
    public final void before() {
        LeakTracker.setContextHint(getTestName());
        logger.info("{}before test", getTestParamsForLogging());
        assertNull("Thread context initialized twice", threadContext);
        if (enableWarningsCheck()) {
            this.threadContext = new ThreadContext(Settings.EMPTY);
            HeaderWarning.setThreadContext(threadContext);
        }
    }

    private static final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    protected static CircuitBreaker newLimitedBreaker(ByteSizeValue max) {
        CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("<es-test-case>", max);
        breakers.add(breaker);
        return breaker;
    }

    @After
    public final void allBreakersMemoryReleased() {
        var breakersToCheck = new ArrayList<>(breakers);
        // We clear it now to avoid keeping old breakers if the assertion fails
        breakers.clear();
        for (CircuitBreaker breaker : breakersToCheck) {
            assertThat(breaker.getUsed(), equalTo(0L));
        }
    }

    /**
     * Whether or not we check after each test whether it has left warnings behind. That happens if any deprecated feature or syntax
     * was used by the test and the test didn't assert on it using {@link #assertWarnings(String...)}.
     */
    protected boolean enableWarningsCheck() {
        return true;
    }

    protected boolean enableBigArraysReleasedCheck() {
        return true;
    }

    @After
    public final void after() throws Exception {
        if (enableBigArraysReleasedCheck()) {
            MockBigArrays.ensureAllArraysAreReleased();
        }
        checkStaticState();
        // We check threadContext != null rather than enableWarningsCheck()
        // because after methods are still called in the event that before
        // methods failed, in which case threadContext might not have been
        // initialized
        if (threadContext != null) {
            ensureNoWarnings();
            HeaderWarning.removeThreadContext(threadContext);
            threadContext = null;
        }
        ensureAllSearchContextsReleased();
        ensureCheckIndexPassed();
        logger.info("{}after test", getTestParamsForLogging());
        LeakTracker.setContextHint("");
    }

    private String getTestParamsForLogging() {
        String name = getTestName();
        int start = name.indexOf('{');
        if (start < 0) return "";
        int end = name.lastIndexOf('}');
        if (end < 0) return "";
        return "[" + name.substring(start + 1, end) + "] ";
    }

    public void ensureNoWarnings() {
        // Check that there are no unaccounted warning headers. These should be checked with {@link #assertWarnings(String...)} in the
        // appropriate test
        try {
            final List<String> warnings = threadContext.getResponseHeaders().get("Warning");
            if (warnings != null) {
                // unit tests do not run with the bundled JDK, if there are warnings we need to filter the no-jdk deprecation warning
                final List<String> filteredWarnings = warnings.stream()
                    .filter(k -> filteredWarnings().stream().noneMatch(s -> k.contains(s)))
                    .collect(Collectors.toList());
                assertThat("unexpected warning headers", filteredWarnings, empty());
            } else {
                assertNull("unexpected warning headers", warnings);
            }
        } finally {
            resetDeprecationLogger();
        }
    }

    protected List<String> filteredWarnings() {
        List<String> filtered = new ArrayList<>();
        filtered.add(
            "Configuring multiple [path.data] paths is deprecated. Use RAID or other system level features for utilizing"
                + " multiple disks. This feature will be removed in a future release."
        );
        filtered.add("Configuring [path.data] with a list is deprecated. Instead specify as a string value");
        filtered.add("setting [path.shared_data] is deprecated and will be removed in a future release");
        return filtered;
    }

    /**
     * Convenience method to assert warnings for settings deprecations and general deprecation warnings.
     * @param settings the settings that are expected to be deprecated
     * @param warnings other expected general deprecation warnings
     */
    protected final void assertSettingDeprecationsAndWarnings(final Setting<?>[] settings, final DeprecationWarning... warnings) {
        assertWarnings(true, Stream.concat(Arrays.stream(settings).map(setting -> {
            Level level = setting.getProperties().contains(Setting.Property.Deprecated) ? DeprecationLogger.CRITICAL : Level.WARN;
            String warningMessage = Strings.format(
                "[%s] setting was deprecated in Elasticsearch and will be removed in a future release. "
                    + "See the %s documentation for the next major version.",
                setting.getKey(),
                (level == Level.WARN) ? "deprecation" : "breaking changes"
            );
            return new DeprecationWarning(level, warningMessage);
        }), Arrays.stream(warnings)).toArray(DeprecationWarning[]::new));
    }

    /**
     * Convenience method to assert warnings for settings deprecations and general deprecation warnings. All warnings passed to this method
     * are assumed to be at WARNING level.
     * @param expectedWarnings expected general deprecation warning messages.
     */
    protected final void assertWarnings(String... expectedWarnings) {
        assertWarnings(
            true,
            Arrays.stream(expectedWarnings)
                .map(expectedWarning -> new DeprecationWarning(Level.WARN, expectedWarning))
                .toArray(DeprecationWarning[]::new)
        );
    }

    /**
     * Convenience method to assert warnings for settings deprecations and general deprecation warnings. All warnings passed to this method
     * are assumed to be at CRITICAL level.
     * @param expectedWarnings expected general deprecation warning messages.
     */
    protected final void assertCriticalWarnings(String... expectedWarnings) {
        assertWarnings(
            true,
            Arrays.stream(expectedWarnings)
                .map(expectedWarning -> new DeprecationWarning(DeprecationLogger.CRITICAL, expectedWarning))
                .toArray(DeprecationWarning[]::new)
        );
    }

    protected final void assertWarnings(boolean stripXContentPosition, DeprecationWarning... expectedWarnings) {
        if (enableWarningsCheck() == false) {
            throw new IllegalStateException("unable to check warning headers if the test is not set to do so");
        }
        try {
            final List<String> actualWarningStrings = threadContext.getResponseHeaders().get("Warning");
            if (expectedWarnings == null || expectedWarnings.length == 0) {
                assertNull("expected 0 warnings, actual: " + actualWarningStrings, actualWarningStrings);
            } else {
                assertNotNull("no warnings, expected: " + Arrays.asList(expectedWarnings), actualWarningStrings);
                final Set<DeprecationWarning> actualDeprecationWarnings = actualWarningStrings.stream().map(warningString -> {
                    String warningText = HeaderWarning.extractWarningValueFromWarningHeader(warningString, stripXContentPosition);
                    final Level level;
                    if (warningString.startsWith(Integer.toString(DeprecationLogger.CRITICAL.intLevel()))) {
                        level = DeprecationLogger.CRITICAL;
                    } else if (warningString.startsWith(Integer.toString(Level.WARN.intLevel()))) {
                        level = Level.WARN;
                    } else {
                        throw new IllegalArgumentException("Unknown level in deprecation message " + warningString);
                    }
                    return new DeprecationWarning(level, warningText);
                }).collect(Collectors.toSet());
                for (DeprecationWarning expectedWarning : expectedWarnings) {
                    DeprecationWarning escapedExpectedWarning = new DeprecationWarning(
                        expectedWarning.level,
                        HeaderWarning.escapeAndEncode(expectedWarning.message)
                    );
                    assertThat(actualDeprecationWarnings, hasItem(escapedExpectedWarning));
                }
                assertEquals(
                    "Expected "
                        + expectedWarnings.length
                        + " warnings but found "
                        + actualWarningStrings.size()
                        + "\nExpected: "
                        + Arrays.asList(expectedWarnings)
                        + "\nActual: "
                        + actualWarningStrings,
                    expectedWarnings.length,
                    actualWarningStrings.size()
                );
            }
        } finally {
            resetDeprecationLogger();
        }
    }

    /**
     * Reset the deprecation logger by clearing the current thread context.
     */
    private void resetDeprecationLogger() {
        // "clear" context by stashing current values and dropping the returned StoredContext
        threadContext.stashContext();
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

    // Tolerate the absence or otherwise denial of these specific lookup classes.
    // At some future time, we should require the JDNI warning.
    private static final List<String> LOG_4J_MSG_PREFIXES = List.of(
        "JNDI lookup class is not available because this JRE does not support JNDI. "
            + "JNDI string lookups will not be available, continuing configuration.",
        "JMX runtime input lookup class is not available because this JRE does not support JMX. "
            + "JMX lookups will not be available, continuing configuration. "
    );

    // separate method so that this can be checked again after suite scoped cluster is shut down
    protected static void checkStaticState() throws Exception {
        // ensure no one changed the status logger level on us
        assertThat(StatusLogger.getLogger().getLevel(), equalTo(Level.WARN));
        synchronized (statusData) {
            try {
                // ensure that there are no status logger messages which would indicate a problem with our Log4j usage; we map the
                // StatusData instances to Strings as otherwise their toString output is useless
                assertThat(
                    statusData.stream().map(status -> status.getMessage().getFormattedMessage()).collect(Collectors.toList()),
                    anyOf(
                        emptyCollectionOf(String.class),
                        contains(startsWith(LOG_4J_MSG_PREFIXES.get(0)), startsWith(LOG_4J_MSG_PREFIXES.get(1))),
                        contains(startsWith(LOG_4J_MSG_PREFIXES.get(1)))
                    )
                );
            } finally {
                // we clear the list so that status data from other tests do not interfere with tests within the same JVM
                statusData.clear();
            }
        }
        synchronized (loggedLeaks) {
            try {
                assertThat(loggedLeaks, empty());
            } finally {
                loggedLeaks.clear();
            }
        }
    }

    /**
     * Assert that at least one leak was detected, also clear the list of detected leaks
     * so the test won't fail for leaks detected up until this point.
     */
    protected static void assertLeakDetected() {
        synchronized (loggedLeaks) {
            assertFalse("No leaks have been detected", loggedLeaks.isEmpty());
            loggedLeaks.clear();
        }
    }

    // this must be a separate method from other ensure checks above so suite scoped integ tests can call...TODO: fix that
    public final void ensureAllSearchContextsReleased() throws Exception {
        assertBusy(() -> MockSearchService.assertNoInFlightContext());
    }

    // mockdirectorywrappers currently set this boolean if checkindex fails
    // TODO: can we do this cleaner???

    /** MockFSDirectoryService sets this: */
    public static final List<Exception> checkIndexFailures = new CopyOnWriteArrayList<>();

    @Before
    public final void resetCheckIndexStatus() throws Exception {
        checkIndexFailures.clear();
    }

    public final void ensureCheckIndexPassed() {
        if (checkIndexFailures.isEmpty() == false) {
            final AssertionError e = new AssertionError("at least one shard failed CheckIndex");
            for (Exception failure : checkIndexFailures) {
                e.addSuppressed(failure);
            }
            throw e;
        }
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
     * @return a random instant between a min and a max value with a random nanosecond precision
     */
    public static Instant randomInstantBetween(Instant minInstant, Instant maxInstant) {
        long epochSecond = randomLongBetween(minInstant.getEpochSecond(), maxInstant.getEpochSecond());
        long minNanos = epochSecond == minInstant.getEpochSecond() ? minInstant.getNano() : 0;
        long maxNanos = epochSecond == maxInstant.getEpochSecond() ? maxInstant.getNano() : 999999999;
        long nanos = randomLongBetween(minNanos, maxNanos);
        return Instant.ofEpochSecond(epochSecond, nanos);
    }

    /**
     * The maximum value that can be represented as an unsigned long.
     */
    public static final BigInteger UNSIGNED_LONG_MAX = BigInteger.ONE.shiftLeft(Long.SIZE).subtract(BigInteger.ONE);

    /**
     * A unsigned long in a {@link BigInteger} between min (inclusive) and max (inclusive).
     */
    public static BigInteger randomUnsignedLongBetween(BigInteger min, BigInteger max) {
        if (min.compareTo(BigInteger.ZERO) < 0) {
            throw new IllegalArgumentException("Must be between [0] and [" + UNSIGNED_LONG_MAX + "]");
        }
        if (0 < max.compareTo(UNSIGNED_LONG_MAX)) {
            throw new IllegalArgumentException("Must be between [0] and [" + UNSIGNED_LONG_MAX + "]");
        }
        // Shift the min and max down into the long range
        long minShifted = min.add(BigInteger.valueOf(Long.MIN_VALUE)).longValueExact();
        long maxShifted = max.add(BigInteger.valueOf(Long.MIN_VALUE)).longValueExact();
        // Grab a random number in that range
        long randomShifted = randomLongBetween(minShifted, maxShifted);
        // Shift back up into long range
        return BigInteger.valueOf(randomShifted).subtract(BigInteger.valueOf(Long.MIN_VALUE));
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
        return rarely() == false;
    }

    public static boolean randomBoolean() {
        return random().nextBoolean();
    }

    public static Boolean randomOptionalBoolean() {
        return randomBoolean() ? Boolean.TRUE : randomFrom(Boolean.FALSE, null);
    }

    public static byte randomByte() {
        return (byte) random().nextInt();
    }

    public static byte randomNonNegativeByte() {
        byte randomByte = randomByte();
        return (byte) (randomByte == Byte.MIN_VALUE ? 0 : Math.abs(randomByte));
    }

    /**
     * Helper method to create a byte array of a given length populated with random byte values
     *
     * @see #randomByte()
     */
    public static byte[] randomByteArrayOfLength(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = randomByte();
        }
        return bytes;
    }

    public static byte randomByteBetween(byte minInclusive, byte maxInclusive) {
        return (byte) randomIntBetween(minInclusive, maxInclusive);
    }

    public static void randomBytesBetween(byte[] bytes, byte minInclusive, byte maxInclusive) {
        for (int i = 0, len = bytes.length; i < len;) {
            bytes[i++] = randomByteBetween(minInclusive, maxInclusive);
        }
    }

    public static BytesReference randomBytesReference(int length) {
        final var slices = new ArrayList<BytesReference>();
        var remaining = length;
        while (remaining > 0) {
            final var sliceLen = between(1, remaining);
            slices.add(new BytesArray(randomByteArrayOfLength(sliceLen)));
            remaining -= sliceLen;
        }
        return CompositeBytesReference.of(slices.toArray(BytesReference[]::new));
    }

    public static short randomShort() {
        return (short) random().nextInt();
    }

    public static int randomInt() {
        return random().nextInt();
    }

    public static IntStream randomInts() {
        return random().ints();
    }

    public static IntStream randomInts(long streamSize) {
        return random().ints(streamSize);
    }

    /**
     * @return a <code>long</code> between <code>0</code> and <code>Long.MAX_VALUE</code> (inclusive) chosen uniformly at random.
     */
    public static long randomNonNegativeLong() {
        return randomLong() & Long.MAX_VALUE;
    }

    /**
     * @return a <code>long</code> between <code>Long.MIN_VALUE</code> and <code>-1</code>  (inclusive) chosen uniformly at random.
     */
    public static long randomNegativeLong() {
        return randomLong() | Long.MIN_VALUE;
    }

    /**
     * @return an <code>int</code> between <code>0</code> and <code>Integer.MAX_VALUE</code> (inclusive) chosen uniformly at random.
     */
    public static int randomNonNegativeInt() {
        return randomInt() & Integer.MAX_VALUE;
    }

    /**
     * @return an <code>int</code> between <code>Integer.MIN_VALUE</code> and <code>-1</code> (inclusive) chosen uniformly at random.
     */
    public static int randomNegativeInt() {
        return randomInt() | Integer.MIN_VALUE;
    }

    public static float randomFloat() {
        return random().nextFloat();
    }

    /**
     * Returns a float value in the interval [start, end) if lowerInclusive is
     * set to true, (start, end) otherwise.
     *
     * @param start          lower bound of interval to draw uniformly distributed random numbers from
     * @param end            upper bound
     * @param lowerInclusive whether or not to include lower end of the interval
     */
    public static float randomFloatBetween(float start, float end, boolean lowerInclusive) {
        float result;

        if (start == -Float.MAX_VALUE || end == Float.MAX_VALUE) {
            // formula below does not work with very large floats
            result = Float.intBitsToFloat(randomInt());
            while (result < start || result > end || Double.isNaN(result)) {
                result = Float.intBitsToFloat(randomInt());
            }
        } else {
            result = randomFloat();
            if (lowerInclusive == false) {
                while (result <= 0.0f) {
                    result = randomFloat();
                }
            }
            result = result * end + (1.0f - result) * start;
        }
        return result;
    }

    public static double randomDouble() {
        return random().nextDouble();
    }

    public static DoubleStream randomDoubles() {
        return random().doubles();
    }

    public static DoubleStream randomDoubles(long streamSize) {
        return random().doubles(streamSize);
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

    public static LongStream randomLongs() {
        return random().longs();
    }

    public static LongStream randomLongs(long streamSize) {
        return random().longs(streamSize);
    }

    /**
     * Returns a random BigInteger uniformly distributed over the range 0 to (2^64 - 1) inclusive
     * Currently BigIntegers are only used for unsigned_long field type, where the max value is 2^64 - 1.
     * Modify this random generator if a wider range for BigIntegers is necessary.
     * @return a random bigInteger in the range [0 ; 2^64 - 1]
     */
    public static BigInteger randomBigInteger() {
        return new BigInteger(64, random());
    }

    /** A random integer from 0..max (inclusive). */
    public static int randomInt(int max) {
        return RandomizedTest.randomInt(max);
    }

    /** A random byte size value. */
    public static ByteSizeValue randomByteSizeValue() {
        return ByteSizeValue.ofBytes(randomLongBetween(0L, Long.MAX_VALUE >> 16));
    }

    /** Pick a random object from the given array. The array must not be empty. */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> T randomFrom(T... array) {
        return randomFrom(random(), array);
    }

    /** Pick a random object from the given array. The array must not be empty. */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> T randomFrom(Random random, T... array) {
        return RandomPicks.randomFrom(random, array);
    }

    /** Pick a random object from the given array of suppliers. The array must not be empty. */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> T randomFrom(Random random, Supplier<T>... array) {
        Supplier<T> supplier = RandomPicks.randomFrom(random, array);
        return supplier.get();
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

    /**
     * Generate a random string containing only alphanumeric characters.
     * <b>The locale for the string is {@link Locale#ROOT}.</b>
     * @param length the length of the string to generate
     * @return the generated string
     */
    public static String randomAlphanumericOfLength(int length) {
        StringBuilder sb = new StringBuilder();
        Random random = random();
        for (int i = 0; i < length; i++) {
            sb.append(ALPHANUMERIC_CHARACTERS.charAt(random.nextInt(ALPHANUMERIC_CHARACTERS.length())));
        }

        return sb.toString();
    }

    public static SecureString randomSecureStringOfLength(int codeUnits) {
        var randomAlpha = randomAlphaOfLength(codeUnits);
        return new SecureString(randomAlpha.toCharArray());
    }

    public static String randomAlphaOfLengthOrNull(int codeUnits) {
        return randomBoolean() ? null : randomAlphaOfLength(codeUnits);
    }

    public static Long randomLongOrNull() {
        return randomBoolean() ? null : randomLong();
    }

    public static Long randomNonNegativeLongOrNull() {
        return randomBoolean() ? null : randomNonNegativeLong();
    }

    public static Integer randomIntOrNull() {
        return randomBoolean() ? null : randomInt();
    }

    public static Integer randomNonNegativeIntOrNull() {
        return randomBoolean() ? null : randomNonNegativeInt();
    }

    public static Float randomFloatOrNull() {
        return randomBoolean() ? null : randomFloat();
    }

    /**
     * Creates a valid random identifier such as node id or index name
     */
    public static String randomIdentifier() {
        return randomAlphaOfLengthBetween(8, 12).toLowerCase(Locale.ROOT);
    }

    /**
     * Returns a project id. This may be {@link Metadata#DEFAULT_PROJECT_ID}, or it may be a randomly-generated id.
     */
    public static ProjectId randomProjectIdOrDefault() {
        return randomBoolean() ? Metadata.DEFAULT_PROJECT_ID : randomUniqueProjectId();
    }

    /**
     * Returns a new randomly-generated project id
     */
    public static ProjectId randomUniqueProjectId() {
        return ProjectId.fromId(randomUUID());
    }

    public static String randomUUID() {
        return UUIDs.randomBase64UUID(random());
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

    /**
     * @param maxArraySize The maximum number of elements in the random array
     * @param stringSize The length of each String in the array
     * @param allowNull Whether the returned array may be null
     * @param allowEmpty Whether the returned array may be empty (have zero elements)
     */
    public static String[] generateRandomStringArray(int maxArraySize, int stringSize, boolean allowNull, boolean allowEmpty) {
        if (allowNull && random().nextBoolean()) {
            return null;
        }
        int arraySize = randomIntBetween(allowEmpty ? 0 : 1, maxArraySize);
        String[] array = new String[arraySize];
        for (int i = 0; i < arraySize; i++) {
            array[i] = RandomStrings.randomAsciiOfLength(random(), stringSize);
        }
        return array;
    }

    public static String[] generateRandomStringArray(int maxArraySize, int stringSize, boolean allowNull) {
        return generateRandomStringArray(maxArraySize, stringSize, allowNull, true);
    }

    public static <T> T[] randomArray(int maxArraySize, IntFunction<T[]> arrayConstructor, Supplier<T> valueConstructor) {
        return randomArray(0, maxArraySize, arrayConstructor, valueConstructor);
    }

    public static <T> T[] randomArray(int minArraySize, int maxArraySize, IntFunction<T[]> arrayConstructor, Supplier<T> valueConstructor) {
        final int size = randomIntBetween(minArraySize, maxArraySize);
        final T[] array = arrayConstructor.apply(size);
        for (int i = 0; i < array.length; i++) {
            array[i] = valueConstructor.get();
        }
        return array;
    }

    public static <T> List<T> randomList(int maxListSize, Supplier<T> valueConstructor) {
        return randomList(0, maxListSize, valueConstructor);
    }

    public static <T> List<T> randomList(int minListSize, int maxListSize, Supplier<T> valueConstructor) {
        final int size = randomIntBetween(minListSize, maxListSize);
        List<T> list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            list.add(valueConstructor.get());
        }
        return list;
    }

    public static <K, V> Map<K, V> randomMap(int minMapSize, int maxMapSize, Supplier<Tuple<K, V>> entryConstructor) {
        final int size = randomIntBetween(minMapSize, maxMapSize);
        Map<K, V> list = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            Tuple<K, V> entry = entryConstructor.get();
            list.put(entry.v1(), entry.v2());
        }
        return list;
    }

    public static <T> Set<T> randomSet(int minSetSize, int maxSetSize, Supplier<T> valueConstructor) {
        return new HashSet<>(randomList(minSetSize, maxSetSize, valueConstructor));
    }

    public static TimeValue randomTimeValue(int lower, int upper, TimeUnit... units) {
        return new TimeValue(between(lower, upper), randomFrom(units));
    }

    public static TimeValue randomTimeValue(int lower, int upper) {
        return randomTimeValue(lower, upper, TimeUnit.values());
    }

    public static TimeValue randomTimeValue() {
        return randomTimeValue(0, 1000);
    }

    public static TimeValue randomPositiveTimeValue() {
        return randomTimeValue(1, 1000);
    }

    /**
     * generate a random epoch millis in a range 1 to 9999-12-31T23:59:59.999
     */
    public static long randomMillisUpToYear9999() {
        return randomLongBetween(1, DateUtils.MAX_MILLIS_BEFORE_9999);
    }

    /**
     * generate a random TimeZone from the ones available in java.util
     */
    public static TimeZone randomTimeZone() {
        return TimeZone.getTimeZone(randomFrom(JAVA_TIMEZONE_IDS));
    }

    /**
     * generate a random TimeZone from the ones available in java.time
     */
    public static ZoneId randomZone() {
        return ZoneId.of(randomFrom(JAVA_ZONE_IDS));
    }

    /**
     * Generate a random valid date formatter pattern.
     */
    public static String randomDateFormatterPattern() {
        return randomFrom(FormatNames.values()).getName();
    }

    /**
     * Generate a random string of at least 112 bits to satisfy minimum entropy requirement when running in FIPS mode.
     */
    public static String randomSecretKey() {
        return randomAlphaOfLengthBetween(14, 20);
    }

    /**
     * Randomly choose between {@link EsExecutors#DIRECT_EXECUTOR_SERVICE} (which does not fork), {@link ThreadPool#generic}, and one of the
     * other named threadpool executors.
     */
    public static Executor randomExecutor(ThreadPool threadPool, String... otherExecutorNames) {
        final var choice = between(0, otherExecutorNames.length + 1);
        if (choice < otherExecutorNames.length) {
            return threadPool.executor(otherExecutorNames[choice]);
        } else if (choice == otherExecutorNames.length) {
            return threadPool.generic();
        } else {
            return EsExecutors.DIRECT_EXECUTOR_SERVICE;
        }
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
     * Runs the code block for the provided interval, waiting for no assertions to trip. Retries on AssertionError
     * with exponential backoff until provided time runs out
     */
    public static void assertBusy(CheckedRunnable<Exception> codeBlock, long maxWaitTime, TimeUnit unit) throws Exception {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        // In case you've forgotten your high-school studies, log10(x) / log10(y) == log y(x)
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

    /**
     * Periodically execute the supplied function until it returns true, or a timeout
     * is reached. This version uses a timeout of 10 seconds. If at all possible,
     * use {@link ESTestCase#assertBusy(CheckedRunnable)} instead.
     *
     * @param breakSupplier determines whether to return immediately or continue waiting.
     * @return the last value returned by <code>breakSupplier</code>
     */
    public static boolean waitUntil(BooleanSupplier breakSupplier) {
        return waitUntil(breakSupplier, 10, TimeUnit.SECONDS);
    }

    // After 1s, we stop growing the sleep interval exponentially and just sleep 1s until maxWaitTime
    private static final long AWAIT_BUSY_THRESHOLD = 1000L;

    /**
     * Periodically execute the supplied function until it returns true, or until the
     * specified maximum wait time has elapsed. If at all possible, use
     * {@link ESTestCase#assertBusy(CheckedRunnable)} instead.
     *
     * @param breakSupplier determines whether to return immediately or continue waiting.
     * @param maxWaitTime the maximum amount of time to wait
     * @param unit the unit of tie for <code>maxWaitTime</code>
     * @return the last value returned by <code>breakSupplier</code>
     */
    public static boolean waitUntil(BooleanSupplier breakSupplier, long maxWaitTime, TimeUnit unit) {
        long maxTimeInMillis = TimeUnit.MILLISECONDS.convert(maxWaitTime, unit);
        long timeInMillis = 1;
        long sum = 0;
        while (sum + timeInMillis < maxTimeInMillis) {
            if (breakSupplier.getAsBoolean()) {
                return true;
            }
            safeSleep(timeInMillis);
            sum += timeInMillis;
            timeInMillis = Math.min(AWAIT_BUSY_THRESHOLD, timeInMillis * 2);
        }
        timeInMillis = maxTimeInMillis - sum;
        safeSleep(Math.max(timeInMillis, 0));
        return breakSupplier.getAsBoolean();
    }

    protected TestThreadPool createThreadPool(ExecutorBuilder<?>... executorBuilders) {
        return new TestThreadPool(getTestName(), executorBuilders);
    }

    public static boolean terminate(ExecutorService... services) {
        boolean terminated = true;
        for (ExecutorService service : services) {
            if (service != null) {
                terminated &= ThreadPool.terminate(service, 10, TimeUnit.SECONDS);
            }
        }
        return terminated;
    }

    public static boolean terminate(ThreadPool threadPool) {
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
        return getResourceDataPath(getClass(), relativePath);
    }

    public static Path getResourceDataPath(Class<?> clazz, String relativePath) {
        final var resource = Objects.requireNonNullElseGet(
            clazz.getResource(relativePath),
            () -> fail(null, "resource not found: [%s][%s]", clazz.getCanonicalName(), relativePath)
        );
        final URI uri;
        try {
            uri = resource.toURI();
        } catch (Exception e) {
            return fail(null, "resource URI not found: [%s][%s]", clazz.getCanonicalName(), relativePath);
        }
        try {
            return PathUtils.get(uri).toAbsolutePath().normalize();
        } catch (Exception e) {
            return fail(e, "resource path not found: %s", uri);
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
        return newNodeEnvironment(Settings.EMPTY);
    }

    public Settings buildEnvSettings(Settings settings) {
        return Settings.builder()
            .put(settings)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
            .putList(Environment.PATH_DATA_SETTING.getKey(), tmpPaths())
            .build();
    }

    public NodeEnvironment newNodeEnvironment(Settings settings) throws IOException {
        Settings build = buildEnvSettings(settings);
        return new NodeEnvironment(build, TestEnvironment.newEnvironment(build));
    }

    public Environment newEnvironment() {
        Settings build = buildEnvSettings(Settings.EMPTY);
        return TestEnvironment.newEnvironment(build);
    }

    public Environment newEnvironment(Settings settings) {
        Settings build = buildEnvSettings(settings);
        return TestEnvironment.newEnvironment(build);
    }

    /** Return consistent index settings for the provided index version. */
    public static Settings.Builder settings(IndexVersion version) {
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version);
    }

    /** Return consistent index settings for the provided index version, shard- and replica-count. */
    public static Settings.Builder indexSettings(IndexVersion indexVersionCreated, int shards, int replicas) {
        return settings(indexVersionCreated).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas);
    }

    /** Return consistent index settings for the provided index version, uuid, shard- and replica-count, */
    public static Settings.Builder indexSettings(IndexVersion indexVersionCreated, String uuid, int shards, int replicas) {
        return settings(indexVersionCreated).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas);
    }

    /** Return consistent index settings for the provided shard- and replica-count. */
    public static Settings.Builder indexSettings(int shards, int replicas) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas);
    }

    /**
     * Returns size random values
     */
    @SafeVarargs
    @SuppressWarnings("varargs")
    public static <T> List<T> randomSubsetOf(int size, T... values) {
        List<T> list = arrayAsArrayList(values);
        return randomSubsetOf(size, list);
    }

    /**
     * Returns a random subset of values (including a potential empty list, or the full original list)
     */
    public static <T> List<T> randomSubsetOf(Collection<T> collection) {
        return randomSubsetOf(randomInt(collection.size()), collection);
    }

    public static <T> List<T> randomNonEmptySubsetOf(Collection<T> collection) {
        if (collection.isEmpty()) {
            throw new IllegalArgumentException("Can't pick non-empty subset of an empty collection");
        }
        return randomSubsetOf(randomIntBetween(1, collection.size()), collection);
    }

    /**
     * Returns size random values
     */
    public static <T> List<T> randomSubsetOf(int size, Collection<T> collection) {
        if (size > collection.size()) {
            throw new IllegalArgumentException(
                "Can't pick " + size + " random objects from a collection of " + collection.size() + " objects"
            );
        }
        List<T> tempList = new ArrayList<>(collection);
        Collections.shuffle(tempList, random());
        return tempList.subList(0, size);
    }

    public static <T> List<T> shuffledList(List<T> list) {
        return randomSubsetOf(list.size(), list);
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

    public static String getTestTransportType() {
        return Netty4Plugin.NETTY_TRANSPORT_NAME;
    }

    public static Class<? extends Plugin> getTestTransportPlugin() {
        return Netty4Plugin.class;
    }

    private static final GeohashGenerator geohashGenerator = new GeohashGenerator();

    public String randomCompatibleMediaType(RestApiVersion version) {
        XContentType type = randomFrom(XContentType.VND_JSON, XContentType.VND_SMILE, XContentType.VND_CBOR, XContentType.VND_YAML);
        return compatibleMediaType(type, version);
    }

    public String compatibleMediaType(XContentType type, RestApiVersion version) {
        if (type.canonical().equals(type)) {
            throw new IllegalArgumentException(
                "Compatible header is only supported for vendor content types."
                    + " You requested "
                    + type.name()
                    + "but likely want VND_"
                    + type.name()
            );
        }
        return type.toParsedMediaType()
            .responseContentTypeHeader(Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME, String.valueOf(version.major)));
    }

    public XContentType randomVendorType() {
        return randomFrom(XContentType.VND_JSON, XContentType.VND_SMILE, XContentType.VND_CBOR, XContentType.VND_YAML);
    }

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
    protected final BytesReference toShuffledXContent(
        ToXContent toXContent,
        XContentType xContentType,
        ToXContent.Params params,
        boolean humanReadable,
        String... exceptFieldNames
    ) throws IOException {
        return toShuffledXContent(toXContent, xContentType, RestApiVersion.current(), params, humanReadable, exceptFieldNames);
    }

    /**
     * Returns the bytes that represent the XContent output of the provided {@link ToXContent} object, using the provided
     * {@link XContentType}. Wraps the output into a new anonymous object according to the value returned
     * by the {@link ToXContent#isFragment()} method returns. Shuffles the keys to make sure that parsing never relies on keys ordering.
     */
    protected final BytesReference toShuffledXContent(
        ToXContent toXContent,
        XContentType xContentType,
        RestApiVersion restApiVersion,
        ToXContent.Params params,
        boolean humanReadable,
        String... exceptFieldNames
    ) throws IOException {
        BytesReference bytes = XContentHelper.toXContent(toXContent, xContentType, restApiVersion, params, humanReadable);
        try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
            try (XContentBuilder builder = shuffleXContent(parser, rarely(), exceptFieldNames)) {
                return BytesReference.bytes(builder);
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
    public static XContentBuilder shuffleXContent(XContentParser parser, boolean prettyPrint, String... exceptFieldNames)
        throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.contentBuilder(parser.contentType());
        if (prettyPrint) {
            xContentBuilder.prettyPrint();
        }
        Token token = parser.currentToken() == null ? parser.nextToken() : parser.currentToken();
        if (token == Token.START_ARRAY) {
            List<Object> shuffledList = shuffleList(parser.listOrderedMap(), new HashSet<>(Arrays.asList(exceptFieldNames)));
            return xContentBuilder.value(shuffledList);
        }
        // we need a sorted map for reproducibility, as we are going to shuffle its keys and write XContent back
        Map<String, Object> shuffledMap = shuffleMap(
            (LinkedHashMap<String, Object>) parser.mapOrdered(),
            new HashSet<>(Arrays.asList(exceptFieldNames))
        );
        return xContentBuilder.map(shuffledMap);
    }

    // shuffle fields of objects in the list, but not the list itself
    @SuppressWarnings("unchecked")
    private static List<Object> shuffleList(List<Object> list, Set<String> exceptFields) {
        List<Object> targetList = new ArrayList<>();
        for (Object value : list) {
            if (value instanceof Map) {
                LinkedHashMap<String, Object> valueMap = (LinkedHashMap<String, Object>) value;
                targetList.add(shuffleMap(valueMap, exceptFields));
            } else if (value instanceof List) {
                targetList.add(shuffleList((List) value, exceptFields));
            } else {
                targetList.add(value);
            }
        }
        return targetList;
    }

    @SuppressWarnings("unchecked")
    public static LinkedHashMap<String, Object> shuffleMap(LinkedHashMap<String, Object> map, Set<String> exceptFields) {
        List<String> keys = new ArrayList<>(map.keySet());
        LinkedHashMap<String, Object> targetMap = new LinkedHashMap<>();
        Collections.shuffle(keys, random());
        for (String key : keys) {
            Object value = map.get(key);
            if (value instanceof Map && exceptFields.contains(key) == false) {
                LinkedHashMap<String, Object> valueMap = (LinkedHashMap<String, Object>) value;
                targetMap.put(key, shuffleMap(valueMap, exceptFields));
            } else if (value instanceof List && exceptFields.contains(key) == false) {
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
    public static <T extends Writeable> T copyWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Writeable.Reader<T> reader
    ) throws IOException {
        return copyWriteable(original, namedWriteableRegistry, reader, TransportVersion.current());
    }

    /**
     * Same as {@link #copyWriteable(Writeable, NamedWriteableRegistry, Writeable.Reader)} but also allows to provide
     * a {@link TransportVersion} argument which will be used to write and read back the object.
     */
    public static <T extends Writeable> T copyWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Writeable.Reader<T> reader,
        TransportVersion version
    ) throws IOException {
        return copyInstance(original, namedWriteableRegistry, StreamOutput::writeWriteable, reader, version);
    }

    /**
     * Create a copy of an original {@link NamedWriteable} object by running it through a {@link BytesStreamOutput} and
     * reading it in again using a provided {@link Writeable.Reader}.
     */
    public static <C extends NamedWriteable, T extends C> C copyNamedWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Class<C> categoryClass
    ) throws IOException {
        return copyNamedWriteable(original, namedWriteableRegistry, categoryClass, TransportVersion.current());
    }

    /**
     * Same as {@link #copyNamedWriteable(NamedWriteable, NamedWriteableRegistry, Class)} but also allows to provide
     * a {@link TransportVersion} argument which will be used to write and read back the object.
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <C extends NamedWriteable, T extends C> C copyNamedWriteable(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Class<C> categoryClass,
        TransportVersion version
    ) throws IOException {
        return copyInstance(
            original,
            namedWriteableRegistry,
            StreamOutput::writeNamedWriteable,
            in -> in.readNamedWriteable(categoryClass),
            version
        );
    }

    protected static <T extends Writeable> T copyInstance(
        T original,
        NamedWriteableRegistry namedWriteableRegistry,
        Writeable.Writer<T> writer,
        Writeable.Reader<T> reader,
        TransportVersion version
    ) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(version);
            writer.write(output, original);
            if (randomBoolean()) {
                try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                    in.setTransportVersion(version);
                    return reader.read(in);
                }
            } else {
                BytesReference bytesReference = output.copyBytes();
                output.reset();
                bytesReference.writeTo(output);
                try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                    in.setTransportVersion(version);
                    return reader.read(in);
                }
            }
        }
    }

    protected final XContentParserConfiguration parserConfig() {
        XContentParserConfiguration config = XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry())
            .withDeprecationHandler(LoggingDeprecationHandler.INSTANCE);
        return randomBoolean() ? config : config.withRestApiVersion(RestApiVersion.minimumSupported());
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContentBuilder builder) throws IOException {
        return createParser(builder.contentType().xContent(), BytesReference.bytes(builder));
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, String data) throws IOException {
        return xContent.createParser(parserConfig(), data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, InputStream data) throws IOException {
        return xContent.createParser(parserConfig(), data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, byte[] data) throws IOException {
        return xContent.createParser(parserConfig(), data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
        return createParser(parserConfig(), xContent, data);
    }

    /**
     * Create a new {@link XContentParser}.
     */
    protected final XContentParser createParser(XContentParserConfiguration config, XContent xContent, BytesReference data)
        throws IOException {
        return XContentHelper.createParserNotCompressed(config, data, xContent.type());
    }

    protected final XContentParser createParserWithCompatibilityFor(XContent xContent, String data, RestApiVersion restApiVersion)
        throws IOException {
        return xContent.createParser(parserConfig().withRestApiVersion(restApiVersion), data);
    }

    private static final NamedXContentRegistry DEFAULT_NAMED_X_CONTENT_REGISTRY = new NamedXContentRegistry(
        CollectionUtils.concatLists(ClusterModule.getNamedXWriteables(), IndicesModule.getNamedXContents())
    );

    /**
     * The {@link NamedXContentRegistry} to use for this test. Subclasses should override and use liberally.
     */
    protected NamedXContentRegistry xContentRegistry() {
        return DEFAULT_NAMED_X_CONTENT_REGISTRY;
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
    public static Script mockScript(String id) {
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
     * Creates an IndexAnalyzers with a single default analyzer
     */
    protected IndexAnalyzers createDefaultIndexAnalyzers() {
        return (type, name) -> {
            if (type == IndexAnalyzers.AnalyzerType.ANALYZER && "default".equals(name)) {
                return Lucene.STANDARD_ANALYZER;
            }
            return null;
        };
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(Index index, Settings settings, AnalysisPlugin... analysisPlugins) throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        return createTestAnalysis(index, nodeSettings, settings, analysisPlugins);
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(Index index, Settings nodeSettings, Settings settings, AnalysisPlugin... analysisPlugins)
        throws IOException {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .build();
        return createTestAnalysis(IndexSettingsModule.newIndexSettings(index, indexSettings), nodeSettings, analysisPlugins);
    }

    /**
     * Creates an TestAnalysis with all the default analyzers configured.
     */
    public static TestAnalysis createTestAnalysis(IndexSettings indexSettings, Settings nodeSettings, AnalysisPlugin... analysisPlugins)
        throws IOException {
        Environment env = TestEnvironment.newEnvironment(nodeSettings);
        AnalysisModule analysisModule = new AnalysisModule(env, Arrays.asList(analysisPlugins), new StablePluginsRegistry());
        AnalysisRegistry analysisRegistry = analysisModule.getAnalysisRegistry();
        return new TestAnalysis(
            analysisRegistry.build(IndexCreationContext.CREATE_INDEX, indexSettings),
            analysisRegistry.buildTokenFilterFactories(indexSettings),
            analysisRegistry.buildTokenizerFactories(indexSettings),
            analysisRegistry.buildCharFilterFactories(indexSettings)
        );
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

        public TestAnalysis(
            IndexAnalyzers indexAnalyzers,
            Map<String, TokenFilterFactory> tokenFilter,
            Map<String, TokenizerFactory> tokenizer,
            Map<String, CharFilterFactory> charFilter
        ) {
            this.indexAnalyzers = indexAnalyzers;
            this.tokenFilter = tokenFilter;
            this.tokenizer = tokenizer;
            this.charFilter = charFilter;
        }
    }

    private static boolean isUnusableLocale() {
        return inFipsJvm()
            && (Locale.getDefault().toLanguageTag().equals("th-TH")
                || Locale.getDefault().toLanguageTag().equals("ja-JP-u-ca-japanese-x-lvariant-JP")
                || Locale.getDefault().toLanguageTag().equals("th-TH-u-nu-thai-x-lvariant-TH"));
    }

    public static boolean inFipsJvm() {
        return Boolean.parseBoolean(System.getProperty(FIPS_SYSPROP));
    }

    /*
     * [NOTE: Port ranges for tests]
     *
     * Some tests involve interactions over the localhost interface of the machine running the tests. The tests run concurrently in multiple
     * JVMs, but all have access to the same network, so there's a risk that different tests will interact with each other in unexpected
     * ways and trigger spurious failures. Gradle numbers its workers sequentially starting at 1 and each worker can determine its own
     * identity from the {@link #TEST_WORKER_SYS_PROPERTY} system property. We use this to try and assign disjoint port ranges to each test
     * worker, avoiding any unexpected interactions, although if we spawn enough test workers then we will wrap around to the beginning
     * again.
     */

    /**
     * Defines the size of the port range assigned to each worker, which must be large enough to supply enough ports to run the tests, but
     * not so large that we run out of ports. See also [NOTE: Port ranges for tests].
     */
    private static final int PORTS_PER_WORKER = 30;

    /**
     * Defines the minimum port that test workers should use. See also [NOTE: Port ranges for tests].
     */
    protected static final int MIN_PRIVATE_PORT = 13301;

    /**
     * Defines the maximum port that test workers should use. See also [NOTE: Port ranges for tests].
     */
    private static final int MAX_PRIVATE_PORT = 32767;

    /**
     * Wrap around after reaching this worker ID.
     */
    private static final int MAX_EFFECTIVE_WORKER_ID = (MAX_PRIVATE_PORT - MIN_PRIVATE_PORT - PORTS_PER_WORKER + 1) / PORTS_PER_WORKER - 1;

    static {
        assert getWorkerBasePort(MAX_EFFECTIVE_WORKER_ID) + PORTS_PER_WORKER - 1 <= MAX_PRIVATE_PORT;
    }

    /**
     * Returns a port range for this JVM according to its Gradle worker ID. See also [NOTE: Port ranges for tests].
     */
    public static String getPortRange() {
        final var firstPort = getWorkerBasePort();
        final var lastPort = firstPort + PORTS_PER_WORKER - 1; // upper bound is inclusive
        assert MIN_PRIVATE_PORT <= firstPort && lastPort <= MAX_PRIVATE_PORT;
        return firstPort + "-" + lastPort;
    }

    /**
     * Returns the start of the port range for this JVM according to its Gradle worker ID. See also [NOTE: Port ranges for tests].
     */
    protected static int getWorkerBasePort() {
        final var workerIdStr = System.getProperty(ESTestCase.TEST_WORKER_SYS_PROPERTY);
        if (workerIdStr == null) {
            // running in IDE
            return MIN_PRIVATE_PORT;
        }

        final var workerId = Integer.parseInt(workerIdStr);
        assert workerId >= 1 : "Non positive gradle worker id: " + workerIdStr;
        return getWorkerBasePort(workerId % (MAX_EFFECTIVE_WORKER_ID + 1));
    }

    private static int getWorkerBasePort(int effectiveWorkerId) {
        assert 0 <= effectiveWorkerId && effectiveWorkerId <= MAX_EFFECTIVE_WORKER_ID;
        // the range [MIN_PRIVATE_PORT, MIN_PRIVATE_PORT+PORTS_PER_WORKER) is only for running outside of Gradle
        return MIN_PRIVATE_PORT + PORTS_PER_WORKER + effectiveWorkerId * PORTS_PER_WORKER;
    }

    public static InetAddress randomIp(boolean v4) {
        try {
            if (v4) {
                byte[] ipv4 = new byte[4];
                random().nextBytes(ipv4);
                return InetAddress.getByAddress(ipv4);
            } else {
                byte[] ipv6 = new byte[16];
                random().nextBytes(ipv6);
                return InetAddress.getByAddress(ipv6);
            }
        } catch (UnknownHostException e) {
            throw new AssertionError();
        }
    }

    public static final class DeprecationWarning {
        private final Level level; // Intentionally ignoring level for the sake of equality for now
        private final String message;

        public DeprecationWarning(Level level, String message) {
            this.level = level;
            this.message = message;
        }

        @Override
        public int hashCode() {
            return Objects.hash(message);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DeprecationWarning that = (DeprecationWarning) o;
            return Objects.equals(message, that.message);
        }

        @Override
        public String toString() {
            return Strings.format("%s: %s", level.name(), message);
        }
    }

    /**
     * Call method at the beginning of a test to disable its execution
     * until a given Lucene version is released and integrated into Elasticsearch
     * @param luceneVersionWithFix the lucene release to wait for
     * @param message an additional message or link with information on the fix
     */
    protected void skipTestWaitingForLuceneFix(org.apache.lucene.util.Version luceneVersionWithFix, String message) {
        final boolean currentVersionHasFix = IndexVersion.current().luceneVersion().onOrAfter(luceneVersionWithFix);
        assumeTrue("Skipping test as it is waiting on a Lucene fix: " + message, currentVersionHasFix);
        fail("Remove call of skipTestWaitingForLuceneFix in " + RandomizedTest.getContext().getTargetMethod());
    }

    /**
     * In non-FIPS mode, get a deterministic SecureRandom SHA1PRNG/SUN instance seeded by deterministic LuceneTestCase.random().
     * In FIPS mode, get a non-deterministic SecureRandom DEFAULT/BCFIPS instance seeded by deterministic LuceneTestCase.random().
     * @return SecureRandom SHA1PRNG instance.
     * @throws NoSuchAlgorithmException SHA1PRNG or DEFAULT algorithm not found.
     * @throws NoSuchProviderException BCFIPS algorithm not found.
     */
    public static SecureRandom secureRandom() throws NoSuchAlgorithmException, NoSuchProviderException {
        return secureRandom(randomByteArrayOfLength(32));
    }

    /**
     * In non-FIPS mode, get a deterministic SecureRandom SHA1PRNG/SUN instance seeded by the input value.
     * In FIPS mode, get a non-deterministic SecureRandom DEFAULT/BCFIPS instance seeded by the input value.
     * @param seed Byte array to use for seeding the SecureRandom instance.
     * @return SecureRandom SHA1PRNG or DEFAULT/BCFIPS instance, depending on FIPS mode.
     * @throws NoSuchAlgorithmException SHA1PRNG or DEFAULT algorithm not found.
     * @throws NoSuchProviderException BCFIPS algorithm not found.
     */
    public static SecureRandom secureRandom(final byte[] seed) throws NoSuchAlgorithmException, NoSuchProviderException {
        return inFipsJvm() ? secureRandomFips(seed) : secureRandomNonFips(seed);
    }

    /**
     * Returns deterministic non-FIPS SecureRandom SHA1PRNG/SUN instance seeded by deterministic LuceneTestCase.random().
     * @return Deterministic non-FIPS SecureRandom SHA1PRNG/SUN instance seeded by deterministic LuceneTestCase.random().
     * @throws NoSuchAlgorithmException Exception if SHA1PRNG algorithm not found, such as missing SUN provider (unlikely).
     */
    protected static SecureRandom secureRandomNonFips() throws NoSuchAlgorithmException {
        return secureRandomNonFips(randomByteArrayOfLength(32));
    }

    /**
     * Returns non-deterministic FIPS SecureRandom DEFAULT/BCFIPS instance. Seeded.
     * @return Non-deterministic FIPS SecureRandom DEFAULT/BCFIPS instance. Seeded.
     * @throws NoSuchAlgorithmException Exception if DEFAULT algorithm not found, such as missing BCFIPS provider.
     */
    protected static SecureRandom secureRandomFips() throws NoSuchAlgorithmException {
        return secureRandomFips(randomByteArrayOfLength(32));
    }

    /**
     * Returns deterministic non-FIPS SecureRandom SHA1PRNG/SUN instance seeded by deterministic LuceneTestCase.random().
     * @return Deterministic non-FIPS SecureRandom SHA1PRNG/SUN instance seeded by deterministic LuceneTestCase.random().
     * @throws NoSuchAlgorithmException Exception if SHA1PRNG algorithm not found, such as missing SUN provider (unlikely).
     */
    protected static SecureRandom secureRandomNonFips(final byte[] seed) throws NoSuchAlgorithmException {
        final SecureRandom secureRandomNonFips = SecureRandom.getInstance("SHA1PRNG"); // SHA1PRNG/SUN
        secureRandomNonFips.setSeed(seed); // SHA1PRNG/SUN setSeed() is deterministic
        return secureRandomNonFips;
    }

    /**
     * Returns non-deterministic FIPS SecureRandom DEFAULT/BCFIPS instance. Seeded.
     * @return Non-deterministic FIPS SecureRandom DEFAULT/BCFIPS instance. Seeded.
     * @throws NoSuchAlgorithmException Exception if DEFAULT algorithm not found, such as missing BCFIPS provider.
     */
    protected static SecureRandom secureRandomFips(final byte[] seed) throws NoSuchAlgorithmException {
        final SecureRandom secureRandomFips = SecureRandom.getInstance("DEFAULT"); // DEFAULT/BCFIPS
        if (WARN_SECURE_RANDOM_FIPS_NOT_DETERMINISTIC.get() == null) {
            WARN_SECURE_RANDOM_FIPS_NOT_DETERMINISTIC.set(Boolean.TRUE);
            final Provider provider = secureRandomFips.getProvider();
            final String providerName = provider.getName();
            final Logger logger = LogManager.getLogger(ESTestCase.class);
            logger.warn(
                "Returning a non-deterministic secureRandom for use with FIPS. "
                    + "This may result in difficulty reproducing test failures with from a given a seed."
            );
        }
        secureRandomFips.setSeed(seed); // DEFAULT/BCFIPS setSeed() is non-deterministic
        return secureRandomFips;
    }

    /**
     * Various timeouts in various REST APIs default to 30s, and many tests do not care about such timeouts, but must specify some value
     * anyway when constructing the corresponding transport/action request instance since we would prefer to avoid having implicit defaults
     * in these requests. This constant can be used as a slightly more meaningful way to refer to the 30s default value in tests.
     */
    public static final TimeValue TEST_REQUEST_TIMEOUT = TimeValue.THIRTY_SECONDS;

    /**
     * The timeout used for the various "safe" wait methods such as {@link #safeAwait} and {@link #safeAcquire}. In tests we generally want
     * these things to complete almost immediately, but sometimes the CI runner executes things rather slowly so we use {@code 10s} as a
     * fairly relaxed definition of "immediately".
     * <p>
     * A well-designed test should not need to wait for anything close to this duration when run in isolation. If you think you need to do
     * so, instead seek a better way to write the test such that it does not need to wait for so long. Tests that take multiple seconds to
     * complete are a big drag on CI times which slows everyone down.
     * <p>
     * For instance, tests which verify things that require the passage of time ought to simulate this (e.g. using a {@link
     * org.elasticsearch.common.util.concurrent.DeterministicTaskQueue}). Excessive busy-waits ought to be replaced by blocking waits. For
     * instance, use a {@link CountDownLatch} or {@link CyclicBarrier} or similar to continue execution as soon as a condition is satisfied.
     * To wait for a particular cluster state, use {@link ClusterServiceUtils#addTemporaryStateListener} rather than busy-waiting on an API.
     */
    public static final TimeValue SAFE_AWAIT_TIMEOUT = TimeValue.timeValueSeconds(10);

    /**
     * Await on the given {@link CyclicBarrier} with a timeout of {@link #SAFE_AWAIT_TIMEOUT}, preserving the thread's interrupt status flag
     * and converting all exceptions into an {@link AssertionError} to trigger a test failure.
     */
    public static void safeAwait(CyclicBarrier barrier) {
        try {
            barrier.await(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e, "safeAwait: interrupted waiting for CyclicBarrier release");
        } catch (Exception e) {
            fail(e, "safeAwait: CyclicBarrier did not release within the timeout");
        }
    }

    /**
     * Await on the given {@link CountDownLatch} with a timeout of {@link #SAFE_AWAIT_TIMEOUT}, preserving the thread's interrupt status
     * flag and asserting that the latch is indeed completed before the timeout.
     */
    public static void safeAwait(CountDownLatch countDownLatch) {
        safeAwait(countDownLatch, SAFE_AWAIT_TIMEOUT);
    }

    /**
     * Await on the given {@link CountDownLatch} with a supplied timeout, preserving the thread's interrupt status
     * flag and asserting that the latch is indeed completed before the timeout.
     * <p>
     * Prefer {@link #safeAwait(CountDownLatch)} (with the default 10s timeout) wherever possible. It's very unusual to need to block a
     * test for more than 10s, and such slow tests are a big problem for overall test suite performance. In almost all cases it's possible
     * to find a different way to write the test which doesn't need such a long wait.
     */
    public static void safeAwait(CountDownLatch countDownLatch, TimeValue timeout) {
        try {
            assertTrue(
                "safeAwait: CountDownLatch did not reach zero within the timeout",
                countDownLatch.await(timeout.millis(), TimeUnit.MILLISECONDS)
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e, "safeAwait: interrupted waiting for CountDownLatch to reach zero");
        }
    }

    /**
     * Acquire a single permit from the given {@link Semaphore}, with a timeout of {@link #SAFE_AWAIT_TIMEOUT}, preserving the thread's
     * interrupt status flag and asserting that the permit was successfully acquired.
     */
    public static void safeAcquire(Semaphore semaphore) {
        safeAcquire(1, semaphore);
    }

    /**
     * Acquire the specified number of permits from the given {@link Semaphore}, with a timeout of {@link #SAFE_AWAIT_TIMEOUT}, preserving
     * the thread's interrupt status flag and asserting that the permits were all successfully acquired.
     */
    public static void safeAcquire(int permits, Semaphore semaphore) {
        try {
            assertTrue(
                "safeAcquire: Semaphore did not acquire permit within the timeout",
                semaphore.tryAcquire(permits, SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS)
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e, "safeAcquire: interrupted waiting for Semaphore to acquire " + permits + " permit(s)");
        }
    }

    /**
     * Wait for the successful completion of the given {@link SubscribableListener}, with a timeout of {@link #SAFE_AWAIT_TIMEOUT},
     * preserving the thread's interrupt status flag and converting all exceptions into an {@link AssertionError} to trigger a test failure.
     *
     * @return The value with which the {@code listener} was completed.
     */
    public static <T> T safeAwait(SubscribableListener<T> listener) {
        return safeAwait(listener, SAFE_AWAIT_TIMEOUT);
    }

    /**
     * Wait for the successful completion of the given {@link SubscribableListener}, respecting the provided timeout,
     * preserving the thread's interrupt status flag and converting all exceptions into an {@link AssertionError} to trigger a test failure.
     *
     * @return The value with which the {@code listener} was completed.
     */
    public static <T> T safeAwait(SubscribableListener<T> listener, TimeValue timeout) {
        final var future = new TestPlainActionFuture<T>();
        listener.addListener(future);
        return safeGet(future, timeout);
    }

    /**
     * Call an async action (a {@link Consumer} of an {@link ActionListener}), wait for it to complete the listener, and then return the
     * result. Preserves the thread's interrupt status flag and converts all exceptions into an {@link AssertionError} to trigger a test
     * failure.
     *
     * @return The value with which the consumed listener was completed.
     */
    public static <T> T safeAwait(CheckedConsumer<ActionListener<T>, ?> consumer) {
        return safeAwait(SubscribableListener.newForked(consumer));
    }

    /**
     * Execute the given {@link ActionRequest} using the given {@link ActionType} and the given {@link ElasticsearchClient}, wait for
     * it to complete with a timeout of {@link #SAFE_AWAIT_TIMEOUT}, and then return the result. An exceptional response, timeout or
     * interrupt triggers a test failure.
     */
    public static <T extends ActionResponse> T safeExecute(ElasticsearchClient client, ActionType<T> action, ActionRequest request) {
        return safeAwait(l -> client.execute(action, request, l));
    }

    /**
     * Wait for the successful completion of the given {@link Future}, with a timeout of {@link #SAFE_AWAIT_TIMEOUT}, preserving the
     * thread's interrupt status flag and converting all exceptions into an {@link AssertionError} to trigger a test failure.
     *
     * @return The value with which the {@code future} was completed.
     */
    public static <T> T safeGet(Future<T> future) {
        return safeGet(future, SAFE_AWAIT_TIMEOUT);
    }

    /**
     * Wait for the successful completion of the given {@link Future}, respecting the provided timeout, preserving the
     * thread's interrupt status flag and converting all exceptions into an {@link AssertionError} to trigger a test failure.
     *
     * @return The value with which the {@code future} was completed.
     */
    // NB private because tests should be designed not to need to wait for longer than SAFE_AWAIT_TIMEOUT.
    private static <T> T safeGet(Future<T> future, TimeValue timeout) {
        try {
            return future.get(timeout.millis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("safeGet: interrupted waiting for SubscribableListener", e);
        } catch (ExecutionException e) {
            throw new AssertionError("safeGet: listener was completed exceptionally", e);
        } catch (TimeoutException e) {
            throw new AssertionError("safeGet: listener was not completed within the timeout", e);
        }
    }

    /**
     * Call a {@link CheckedSupplier}, converting all exceptions into an {@link AssertionError}. Useful for avoiding
     * try/catch boilerplate or cumbersome propagation of checked exceptions around something that <i>should</i> never throw.
     *
     * @return The value returned by the {@code supplier}.
     */
    public static <T> T safeGet(CheckedSupplier<T, ?> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            return fail(e);
        }
    }

    /**
     * Wait for the exceptional completion of the given {@link SubscribableListener}, with a timeout of {@link #SAFE_AWAIT_TIMEOUT},
     * preserving the thread's interrupt status flag and converting a successful completion, interrupt or timeout into an {@link
     * AssertionError} to trigger a test failure.
     *
     * @return The exception with which the {@code listener} was completed exceptionally.
     */
    public static Exception safeAwaitFailure(SubscribableListener<?> listener) {
        return safeAwait(exceptionListener -> listener.addListener(ActionTestUtils.assertNoSuccessListener(exceptionListener::onResponse)));
    }

    /**
     * Wait for the exceptional completion of the given async action, with a timeout of {@link #SAFE_AWAIT_TIMEOUT},
     * preserving the thread's interrupt status flag and converting a successful completion, interrupt or timeout into an {@link
     * AssertionError} to trigger a test failure.
     *
     * @return The exception with which the {@code listener} was completed exceptionally.
     */
    public static <T> Exception safeAwaitFailure(Consumer<ActionListener<T>> consumer) {
        return safeAwait(exceptionListener -> consumer.accept(ActionTestUtils.assertNoSuccessListener(exceptionListener::onResponse)));
    }

    /**
     * Wait for the exceptional completion of the given async action, with a timeout of {@link #SAFE_AWAIT_TIMEOUT},
     * preserving the thread's interrupt status flag and converting a successful completion, interrupt or timeout into an {@link
     * AssertionError} to trigger a test failure.
     *
     * @param responseType Class of listener response type, to aid type inference but otherwise ignored.
     *
     * @return The exception with which the {@code listener} was completed exceptionally.
     */
    public static <T> Exception safeAwaitFailure(@SuppressWarnings("unused") Class<T> responseType, Consumer<ActionListener<T>> consumer) {
        return safeAwaitFailure(consumer);
    }

    /**
     * Wait for the exceptional completion of the given async action, with a timeout of {@link #SAFE_AWAIT_TIMEOUT},
     * preserving the thread's interrupt status flag and converting a successful completion, interrupt or timeout into an {@link
     * AssertionError} to trigger a test failure.
     *
     * @param responseType  Class of listener response type, to aid type inference but otherwise ignored.
     * @param exceptionType Expected exception type. This method throws an {@link AssertionError} if a different type of exception is seen.
     *
     * @return The exception with which the {@code listener} was completed exceptionally.
     */
    public static <Response, ExpectedException extends Exception> ExpectedException safeAwaitFailure(
        Class<ExpectedException> exceptionType,
        Class<Response> responseType,
        Consumer<ActionListener<Response>> consumer
    ) {
        return asInstanceOf(exceptionType, safeAwaitFailure(responseType, consumer));
    }

    /**
     * Wait for the exceptional completion of the given async action, with a timeout of {@link #SAFE_AWAIT_TIMEOUT},
     * preserving the thread's interrupt status flag and converting a successful completion, interrupt or timeout into an {@link
     * AssertionError} to trigger a test failure. Any layers of {@link ElasticsearchWrapperException} are removed from the thrown exception
     * using {@link ExceptionsHelper#unwrapCause}.
     *
     * @param responseType  Class of listener response type, to aid type inference but otherwise ignored.
     * @param exceptionType Expected unwrapped exception type. This method throws an {@link AssertionError} if a different type of exception
     *                      is seen.
     *
     * @return The unwrapped exception with which the {@code listener} was completed exceptionally.
     */
    public static <Response, ExpectedException extends Exception> ExpectedException safeAwaitAndUnwrapFailure(
        Class<ExpectedException> exceptionType,
        Class<Response> responseType,
        Consumer<ActionListener<Response>> consumer
    ) {
        return asInstanceOf(exceptionType, ExceptionsHelper.unwrapCause(safeAwaitFailure(responseType, consumer)));
    }

    /**
     * Send the current thread to sleep for the given duration, asserting that the sleep is not interrupted but preserving the thread's
     * interrupt status flag in any case.
     */
    public static void safeSleep(TimeValue timeValue) {
        safeSleep(timeValue.millis());
    }

    /**
     * Send the current thread to sleep for the given number of milliseconds, asserting that the sleep is not interrupted but preserving the
     * thread's interrupt status flag in any case.
     */
    public static void safeSleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(e, "safeSleep: interrupted");
        }
    }

    /**
     * Wait for all tasks currently running or enqueued on the given executor to complete.
     */
    public static void flushThreadPoolExecutor(ThreadPool threadPool, String executorName) {
        final var maxThreads = threadPool.info(executorName).getMax();
        final var barrier = new CyclicBarrier(maxThreads + 1);
        final var executor = threadPool.executor(executorName);
        for (int i = 0; i < maxThreads; i++) {
            executor.execute(new AbstractRunnable() {
                @Override
                protected void doRun() {
                    safeAwait(barrier);
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e, "unexpected");
                }

                @Override
                public boolean isForceExecution() {
                    return true;
                }
            });
        }
        safeAwait(barrier);
    }

    protected static boolean isTurkishLocale() {
        return Locale.getDefault().getLanguage().equals(new Locale("tr").getLanguage())
            || Locale.getDefault().getLanguage().equals(new Locale("az").getLanguage());
    }

    /*
     * Assert.assertThat (inherited from LuceneTestCase superclass) has been deprecated.
     * So make sure that all assertThat references use the non-deprecated version.
     */
    public static <T> void assertThat(T actual, Matcher<? super T> matcher) {
        MatcherAssert.assertThat(actual, matcher);
    }

    public static <T> void assertThat(String reason, T actual, Matcher<? super T> matcher) {
        MatcherAssert.assertThat(reason, actual, matcher);
    }

    public static <T> T fail(Throwable t, String msg, Object... args) {
        throw new AssertionError(org.elasticsearch.common.Strings.format(msg, args), t);
    }

    public static <T> T fail(Throwable t) {
        return fail(t, "unexpected");
    }

    @SuppressWarnings("unchecked")
    public static <T> T asInstanceOf(Class<T> clazz, Object o) {
        assertThat(o, Matchers.instanceOf(clazz));
        return (T) o;
    }

    public static <T extends Throwable> T expectThrows(Class<T> expectedType, ActionFuture<? extends RefCounted> future) {
        return expectThrows(
            expectedType,
            "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown",
            () -> future.actionGet().decRef()  // dec ref if we unexpectedly fail to not leak transport response
        );
    }

    public static <T extends Throwable> T expectThrows(Class<T> expectedType, RequestBuilder<?, ?> builder) {
        return expectThrows(
            expectedType,
            "Expected exception " + expectedType.getSimpleName() + " but no exception was thrown",
            () -> builder.get().decRef() // dec ref if we unexpectedly fail to not leak transport response
        );
    }

    /**
     * Checks a specific exception class with matched message is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(Class<T> expectedType, Matcher<String> messageMatcher, ThrowingRunnable runnable) {
        var e = expectThrows(expectedType, runnable);
        assertThat(e.getMessage(), messageMatcher);
        return e;
    }

    /**
     * Checks a specific exception class with matched message is thrown by the given runnable, and returns it.
     */
    public static <T extends Throwable> T expectThrows(
        String reason,
        Class<T> expectedType,
        Matcher<String> messageMatcher,
        ThrowingRunnable runnable
    ) {
        var e = expectThrows(expectedType, reason, runnable);
        assertThat(reason, e.getMessage(), messageMatcher);
        return e;
    }

    /**
     * Same as {@link #runInParallel(int, IntConsumer)} but also attempts to start all tasks at the same time by blocking execution on a
     * barrier until all threads are started and ready to execute their task.
     */
    public static void startInParallel(int numberOfTasks, IntConsumer taskFactory) {
        final CyclicBarrier barrier = new CyclicBarrier(numberOfTasks);
        runInParallel(numberOfTasks, i -> {
            safeAwait(barrier);
            taskFactory.accept(i);
        });
    }

    /**
     * Run {@code numberOfTasks} parallel tasks that were created by the given {@code taskFactory}. On of the tasks will be run on the
     * calling thread, the rest will be run on a new thread.
     * @param numberOfTasks number of tasks to run in parallel
     * @param taskFactory task factory
     */
    public static void runInParallel(int numberOfTasks, IntConsumer taskFactory) {
        final ArrayList<Future<?>> futures = new ArrayList<>(numberOfTasks);
        final Thread[] threads = new Thread[numberOfTasks - 1];
        for (int i = 0; i < numberOfTasks; i++) {
            final int index = i;
            var future = new FutureTask<Void>(() -> taskFactory.accept(index), null);
            futures.add(future);
            if (i == numberOfTasks - 1) {
                future.run();
            } else {
                threads[i] = new Thread(future);
                threads[i].setName("runInParallel-T#" + i);
                threads[i].start();
            }
        }
        Exception e = null;
        try {
            for (Thread thread : threads) {
                // no sense in waiting for the rest of the threads, nor any futures, if interrupted, just bail out and fail
                thread.join();
            }
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (InterruptedException interruptedException) {
                    // no sense in waiting for the rest of the futures if interrupted, just bail out and fail
                    Thread.currentThread().interrupt();
                    throw interruptedException;
                } catch (Exception executionException) {
                    e = ExceptionsHelper.useOrSuppress(e, executionException);
                }
            }
        } catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            e = ExceptionsHelper.useOrSuppress(e, interruptedException);
        }
        if (e != null) {
            throw new AssertionError(e);
        }
    }

    public static void ensureAllContextsReleased(SearchService searchService) {
        try {
            assertBusy(() -> {
                assertThat(searchService.getActiveContexts(), equalTo(0));
                assertThat(searchService.getOpenScrollContexts(), equalTo(0));
            });
        } catch (Exception e) {
            throw new AssertionError("Failed to verify search contexts", e);
        }
    }

    /**
     * Create a new searcher over the reader. This searcher might randomly use threads.
     * Provides the same functionality as {@link LuceneTestCase#newSearcher(IndexReader)},
     * with the only difference that concurrency will only ever be inter-segment and never intra-segment.
     */
    public static IndexSearcher newSearcher(IndexReader r) {
        return newSearcher(r, true);
    }

    /**
     * Create a new searcher over the reader. This searcher might randomly use threads.
     * Provides the same functionality as {@link LuceneTestCase#newSearcher(IndexReader, boolean)},
     * with the only difference that concurrency will only ever be inter-segment and never intra-segment.
     */
    public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap) {
        return newSearcher(r, maybeWrap, true);
    }

    /**
     * Create a new searcher over the reader. This searcher might randomly use threads.
     * Provides the same functionality as {@link LuceneTestCase#newSearcher(IndexReader, boolean, boolean)},
     * with the only difference that concurrency will only ever be inter-segment and never intra-segment.
     */
    public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap, boolean wrapWithAssertions) {
        return newSearcher(r, maybeWrap, wrapWithAssertions, randomBoolean());
    }

    /**
     * Create a new searcher over the reader.
     * Provides the same functionality as {@link LuceneTestCase#newSearcher(IndexReader, boolean, boolean, boolean)},
     * with the only difference that concurrency will only ever be inter-segment and never intra-segment.
     */
    public static IndexSearcher newSearcher(IndexReader r, boolean maybeWrap, boolean wrapWithAssertions, boolean useThreads) {
        if (useThreads) {
            return newSearcher(r, maybeWrap, wrapWithAssertions, Concurrency.INTER_SEGMENT);
        }
        return newSearcher(r, maybeWrap, wrapWithAssertions, Concurrency.NONE);
    }
}
