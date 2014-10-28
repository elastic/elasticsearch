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

package org.apache.lucene.util;

import com.carrotsearch.randomizedtesting.*;
import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.rules.NoClassHooksShadowingRule;
import com.carrotsearch.randomizedtesting.rules.NoInstanceHooksOverridesRule;
import com.carrotsearch.randomizedtesting.rules.StaticFieldsInvariantRule;
import com.carrotsearch.randomizedtesting.rules.SystemPropertiesInvariantRule;
import org.apache.lucene.util.LuceneTestCase.SuppressCodecs;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.CurrentTestFailedMarker;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import java.io.Closeable;
import java.io.File;
import java.lang.annotation.*;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

@TestMethodProviders({
        LuceneJUnit3MethodProvider.class,
        JUnit4MethodProvider.class
})
@Listeners({
        ReproduceInfoPrinter.class,
        FailureMarker.class,
        CurrentTestFailedMarker.class
})
@RunWith(value = com.carrotsearch.randomizedtesting.RandomizedRunner.class)
@SuppressCodecs(value = "Lucene3x")

// NOTE: this class is in o.a.lucene.util since it uses some classes that are related
// to the test framework that didn't make sense to copy but are package private access
public abstract class AbstractRandomizedTest extends RandomizedTest {


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

    /**
     * Use this constant when creating Analyzers and any other version-dependent stuff.
     * <p><b>NOTE:</b> Change this when development starts for new Lucene version:
     */
    public static final Version TEST_VERSION_CURRENT = Lucene.VERSION;

    /**
     * True if and only if tests are run in verbose mode. If this flag is false
     * tests are not expected to print any messages.
     */
    public static final boolean VERBOSE = systemPropertyAsBoolean("tests.verbose", false);

    /**
     * A random multiplier which you should use when writing random tests:
     * multiply it by the number of iterations to scale your tests (for nightly builds).
     */
    public static final int RANDOM_MULTIPLIER = systemPropertyAsInt("tests.multiplier", 1);

    /**
     * TODO: javadoc?
     */
    public static final String DEFAULT_LINE_DOCS_FILE = "europarl.lines.txt.gz";

    /**
     * the line file used by LineFileDocs
     */
    public static final String TEST_LINE_DOCS_FILE = System.getProperty("tests.linedocsfile", DEFAULT_LINE_DOCS_FILE);

    /**
     * Create indexes in this directory, optimally use a subdir, named after the test
     */
    public static final File TEMP_DIR;

    public static final int TESTS_PROCESSORS;

    static {
        String s = System.getProperty("tempDir", System.getProperty("java.io.tmpdir"));
        if (s == null)
            throw new RuntimeException("To run tests, you need to define system property 'tempDir' or 'java.io.tmpdir'.");
        TEMP_DIR = new File(s);
        TEMP_DIR.mkdirs();

        String processors = System.getProperty(SYSPROP_PROCESSORS, ""); // mvn sets "" as default
        if (processors == null || processors.isEmpty()) {
            processors = Integer.toString(EsExecutors.boundedNumberOfProcessors(ImmutableSettings.EMPTY));
        }
        TESTS_PROCESSORS = Integer.parseInt(processors);
    }

    /**
     * These property keys will be ignored in verification of altered properties.
     *
     * @see SystemPropertiesInvariantRule
     * @see #ruleChain
     * @see #classRules
     */
    private static final String[] IGNORED_INVARIANT_PROPERTIES = {
            "user.timezone", "java.rmi.server.randomIDs", "sun.nio.ch.bugLevel",
            "solr.directoryFactory", "solr.solr.home", "solr.data.dir" // these might be set by the LuceneTestCase -- ignore
    };

    // -----------------------------------------------------------------
    // Fields initialized in class or instance rules.
    // -----------------------------------------------------------------


    // -----------------------------------------------------------------
    // Class level (suite) rules.
    // -----------------------------------------------------------------

    /**
     * Stores the currently class under test.
     */
    private static final TestRuleStoreClassName classNameRule;

    /**
     * Class environment setup rule.
     */
    static final TestRuleSetupAndRestoreClassEnv classEnvRule;

    /**
     * Suite failure marker (any error in the test or suite scope).
     */
    public final static TestRuleMarkFailure suiteFailureMarker =
            new TestRuleMarkFailure();

    /**
     * Ignore tests after hitting a designated number of initial failures. This
     * is truly a "static" global singleton since it needs to span the lifetime of all
     * test classes running inside this JVM (it cannot be part of a class rule).
     * <p/>
     * <p>This poses some problems for the test framework's tests because these sometimes
     * trigger intentional failures which add up to the global count. This field contains
     * a (possibly) changing reference to {@link TestRuleIgnoreAfterMaxFailures} and we
     * dispatch to its current value from the {@link #classRules} chain using {@link TestRuleDelegate}.
     */
    private static final AtomicReference<TestRuleIgnoreAfterMaxFailures> ignoreAfterMaxFailuresDelegate;
    private static final TestRule ignoreAfterMaxFailures;

    static {
        int maxFailures = systemPropertyAsInt(SYSPROP_MAXFAILURES, Integer.MAX_VALUE);
        boolean failFast = systemPropertyAsBoolean(SYSPROP_FAILFAST, false);

        if (failFast) {
            if (maxFailures == Integer.MAX_VALUE) {
                maxFailures = 1;
            } else {
                Logger.getLogger(LuceneTestCase.class.getSimpleName()).warning(
                        "Property '" + SYSPROP_MAXFAILURES + "'=" + maxFailures + ", 'failfast' is" +
                                " ignored.");
            }
        }

        ignoreAfterMaxFailuresDelegate =
                new AtomicReference<>(
                        new TestRuleIgnoreAfterMaxFailures(maxFailures));
        ignoreAfterMaxFailures = TestRuleDelegate.of(ignoreAfterMaxFailuresDelegate);
    }

    /**
     * Temporarily substitute the global {@link TestRuleIgnoreAfterMaxFailures}. See
     * {@link #ignoreAfterMaxFailuresDelegate} for some explanation why this method
     * is needed.
     */
    public static TestRuleIgnoreAfterMaxFailures replaceMaxFailureRule(TestRuleIgnoreAfterMaxFailures newValue) {
        return ignoreAfterMaxFailuresDelegate.getAndSet(newValue);
    }

    /**
     * Max 10mb of static data stored in a test suite class after the suite is complete.
     * Prevents static data structures leaking and causing OOMs in subsequent tests.
     */
    private final static long STATIC_LEAK_THRESHOLD = 10 * 1024 * 1024;

    /**
     * By-name list of ignored types like loggers etc.
     */
    private final static Set<String> STATIC_LEAK_IGNORED_TYPES =
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
                    EnumSet.class.getName())));

    private final static Set<Class<?>> TOP_LEVEL_CLASSES =
            Collections.unmodifiableSet(new HashSet<Class<?>>(Arrays.asList(
                    AbstractRandomizedTest.class, LuceneTestCase.class,
                    ElasticsearchIntegrationTest.class, ElasticsearchTestCase.class)));

    /**
     * This controls how suite-level rules are nested. It is important that _all_ rules declared
     * in {@link LuceneTestCase} are executed in proper order if they depend on each
     * other.
     */
    @ClassRule
    public static TestRule classRules = RuleChain
            .outerRule(new TestRuleIgnoreTestSuites())
            .around(ignoreAfterMaxFailures)
            .around(suiteFailureMarker)
            .around(new TestRuleAssertionsRequired())
            .around(new StaticFieldsInvariantRule(STATIC_LEAK_THRESHOLD, true) {
                @Override
                protected boolean accept(java.lang.reflect.Field field) {
                    // Don't count known classes that consume memory once.
                    if (STATIC_LEAK_IGNORED_TYPES.contains(field.getType().getName())) {
                        return false;
                    }
                    // Don't count references from ourselves, we're top-level.
                    if (TOP_LEVEL_CLASSES.contains(field.getDeclaringClass())) {
                        return false;
                    }
                    return super.accept(field);
                }
            })
            .around(new NoClassHooksShadowingRule())
            .around(new NoInstanceHooksOverridesRule() {
                @Override
                protected boolean verify(Method key) {
                    String name = key.getName();
                    return !(name.equals("setUp") || name.equals("tearDown"));
                }
            })
            .around(new SystemPropertiesInvariantRule(IGNORED_INVARIANT_PROPERTIES))
            .around(classNameRule = new TestRuleStoreClassName())
            .around(classEnvRule = new TestRuleSetupAndRestoreClassEnv());


    // -----------------------------------------------------------------
    // Test level rules.
    // -----------------------------------------------------------------

    /**
     * Enforces {@link #setUp()} and {@link #tearDown()} calls are chained.
     */
    private TestRuleSetupTeardownChained parentChainCallRule = new TestRuleSetupTeardownChained();

    /**
     * Save test thread and name.
     */
    private TestRuleThreadAndTestName threadAndTestNameRule = new TestRuleThreadAndTestName();

    /**
     * Taint suite result with individual test failures.
     */
    private TestRuleMarkFailure testFailureMarker = new TestRuleMarkFailure(suiteFailureMarker);

    /**
     * This controls how individual test rules are nested. It is important that
     * _all_ rules declared in {@link LuceneTestCase} are executed in proper order
     * if they depend on each other.
     */
    @Rule
    public final TestRule ruleChain = RuleChain
            .outerRule(testFailureMarker)
            .around(ignoreAfterMaxFailures)
            .around(threadAndTestNameRule)
            .around(new SystemPropertiesInvariantRule(IGNORED_INVARIANT_PROPERTIES))
            .around(new TestRuleSetupAndRestoreInstanceEnv())
            .around(new TestRuleFieldCacheSanity())
            .around(parentChainCallRule);

    // -----------------------------------------------------------------
    // Suite and test case setup/ cleanup.
    // -----------------------------------------------------------------

    /** MockFSDirectoryService sets this: */
    public static boolean checkIndexFailed;

    /**
     * For subclasses to override. Overrides must call {@code super.setUp()}.
     */
    @Before
    public void setUp() throws Exception {
        parentChainCallRule.setupCalled = true;
        checkIndexFailed = false;
    }

    /**
     * For subclasses to override. Overrides must call {@code super.tearDown()}.
     */
    @After
    public void tearDown() throws Exception {
        parentChainCallRule.teardownCalled = true;
        assertFalse("at least one shard failed CheckIndex", checkIndexFailed);
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

    /**
     * Return the current class being tested.
     */
    public static Class<?> getTestClass() {
        return classNameRule.getTestClass();
    }

    /**
     * Return the name of the currently executing test case.
     */
    public String getTestName() {
        return threadAndTestNameRule.testMethodName;
    }
}
