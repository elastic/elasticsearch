/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.test;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;
import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.Timeout;

import org.apache.http.client.fluent.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.FileMatcher;
import org.elasticsearch.packaging.util.FileUtils;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.Platforms;
import org.elasticsearch.packaging.util.ServerUtils;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.packaging.util.docker.Docker;
import org.elasticsearch.packaging.util.docker.DockerFileMatcher;
import org.elasticsearch.packaging.util.docker.DockerShell;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.elasticsearch.packaging.util.Cleanup.cleanEverything;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.Directory;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p750;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.rm;
import static org.elasticsearch.packaging.util.docker.Docker.copyFromContainer;
import static org.elasticsearch.packaging.util.docker.Docker.ensureImageIsLoaded;
import static org.elasticsearch.packaging.util.docker.Docker.removeContainer;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Class that all packaging test cases should inherit from
 */
@RunWith(RandomizedRunner.class)
@TestMethodProviders({ JUnit3MethodProvider.class })
@Timeout(millis = 20 * 60 * 1000) // 20 min
@TestCaseOrdering(TestCaseOrdering.AlphabeticOrder.class)
public abstract class PackagingTestCase extends Assert {

    /**
     * Annotation for tests which exhibit a known issue and are temporarily disabled.
     */
    @Documented
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @TestGroup(enabled = false, sysProperty = "tests.awaitsfix")
    @interface AwaitsFix {
        /** Point to JIRA entry. */
        String bugUrl();
    }

    protected final Logger logger = LogManager.getLogger(getClass());

    // the distribution being tested
    protected static final Distribution distribution;
    static {
        distribution = new Distribution(Paths.get(System.getProperty("tests.distribution")));
    }

    // the java installation already installed on the system
    protected static final String systemJavaHome;
    static {
        Shell initShell = new Shell();
        if (Platforms.WINDOWS) {
            systemJavaHome = initShell.run("$Env:SYSTEM_JAVA_HOME").stdout().trim();
        } else {
            assert Platforms.LINUX || Platforms.DARWIN;
            systemJavaHome = initShell.run("echo $SYSTEM_JAVA_HOME").stdout().trim();
        }
    }

    // the current installation of the distribution being tested
    protected static Installation installation;
    protected static Tuple<String, String> fileSuperuserForInstallation;

    private static boolean failed;

    @Rule
    public final TestWatcher testFailureRule = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            failed = true;
        }
    };

    // a shell to run system commands with
    protected static Shell sh;

    @Rule
    public final TestName testNameRule = new TestName();

    @BeforeClass
    public static void init() throws Exception {
        assumeTrue("only compatible distributions", distribution.packaging.compatible);

        // make sure temp dir exists
        if (Files.exists(getRootTempDir()) == false) {
            Files.createDirectories(getRootTempDir());
        }

        // cleanup from previous test
        cleanup();

        // create shell
        if (distribution().isDocker()) {
            ensureImageIsLoaded(distribution);
            sh = new DockerShell();
        } else {
            sh = new Shell();
        }
    }

    @AfterClass
    public static void cleanupDocker() {
        if (distribution().isDocker()) {
            // runContainer also calls this, so we don't need this method to be annotated as `@After`
            removeContainer();
        }
    }

    @Before
    public void setup() throws Exception {
        assumeFalse(failed); // skip rest of tests once one fails

        sh.reset();
        if (distribution().hasJdk == false) {
            Platforms.onLinux(() -> sh.getEnv().put("ES_JAVA_HOME", systemJavaHome));
            Platforms.onWindows(() -> sh.getEnv().put("ES_JAVA_HOME", systemJavaHome));
        }
        if (installation != null
            && installation.distribution.isDocker() == false
            && Version.fromString(installation.distribution.baseVersion).onOrAfter(Version.V_7_11_0)) {
            // Explicitly set heap for versions 7.11 and later otherwise auto heap sizing will cause OOM issues
            setHeap("1g");
        }
    }

    @After
    public void teardown() throws Exception {
        // move log file so we can avoid false positives when grepping for
        // messages in logs during test
        if (installation != null && failed == false) {

            if (Files.exists(installation.logs)) {
                Path logFile = installation.logs.resolve("elasticsearch.log");
                String prefix = this.getClass().getSimpleName() + "." + testNameRule.getMethodName();
                if (Files.exists(logFile)) {
                    Path newFile = installation.logs.resolve(prefix + ".elasticsearch.log");
                    try {
                        FileUtils.mv(logFile, newFile);
                    } catch (Exception e) {
                        // There was a problem cleaning up log files. This usually means Windows wackiness
                        // where something still has the file open. Here we dump what we can of the log files to see
                        // if ES is still running.
                        dumpDebug();
                        throw e;
                    }
                }
                for (Path rotatedLogFile : FileUtils.lsGlob(installation.logs, "elasticsearch*.tar.gz")) {
                    Path newRotatedLogFile = installation.logs.resolve(prefix + "." + rotatedLogFile.getFileName());
                    FileUtils.mv(rotatedLogFile, newRotatedLogFile);
                }
            }
        }

    }

    /** The {@link Distribution} that should be tested in this case */
    protected static Distribution distribution() {
        return distribution;
    }

    protected static void install() throws Exception {
        switch (distribution.packaging) {
            case TAR, ZIP -> {
                installation = Archives.installArchive(sh, distribution);
                Archives.verifyArchiveInstallation(installation, distribution);
            }
            case DEB, RPM -> {
                installation = Packages.installPackage(sh, distribution);
                Packages.verifyPackageInstallation(installation, distribution, sh);
            }
            case DOCKER, DOCKER_UBI, DOCKER_IRON_BANK, DOCKER_CLOUD, DOCKER_CLOUD_ESS, DOCKER_WOLFI -> {
                installation = Docker.runContainer(distribution);
                Docker.verifyContainerInstallation(installation);
            }
            default -> throw new IllegalStateException("Unknown Elasticsearch packaging type.");
        }

        // the purpose of the packaging tests are not to all test auto heap, so we explicitly set heap size to 1g
        if (distribution.isDocker() == false) {
            setHeap("1g");
        }
    }

    protected static void cleanup() throws Exception {
        installation = null;
        fileSuperuserForInstallation = null;
        cleanEverything();
    }

    /**
     * Prints all available information about the installed Elasticsearch process, including pid, logs and stdout/stderr.
     */
    protected void dumpDebug() {
        if (Files.exists(installation.home.resolve("elasticsearch.pid"))) {
            String pid = FileUtils.slurp(installation.home.resolve("elasticsearch.pid")).trim();
            logger.info("Dumping jstack of elasticsearch processb ({}) that failed to start", pid);
            sh.runIgnoreExitCode("jstack " + pid);
        }
        if (Files.exists(installation.logs.resolve("elasticsearch.log"))) {
            logger.warn("Elasticsearch log:\n" + FileUtils.slurpAllLogs(installation.logs, "elasticsearch.log", "*.log.gz"));
        }
        if (Files.exists(installation.logs.resolve("output.out"))) {
            logger.warn("Stdout:\n" + FileUtils.slurpTxtorGz(installation.logs.resolve("output.out")));
        }
        if (Files.exists(installation.logs.resolve("output.err"))) {
            logger.warn("Stderr:\n" + FileUtils.slurpTxtorGz(installation.logs.resolve("output.err")));
        }
    }

    /**
     * Starts and stops elasticsearch, and performs assertions while it is running.
     */
    protected void assertWhileRunning(Platforms.PlatformAction assertions) throws Exception {
        try {
            awaitElasticsearchStartup(runElasticsearchStartCommand(null, true, false));
        } catch (AssertionError | Exception e) {
            dumpDebug();
            throw e;
        }

        try {
            assertions.run();
        } catch (AssertionError | Exception e) {
            dumpDebug();
            throw e;
        }
        stopElasticsearch();
    }

    /**
     * Run the command to start Elasticsearch, but don't wait or test for success.
     * This method is useful for testing failure conditions in startup. To await success,
     * use {@link #startElasticsearch()}.
     * @param password Password for password-protected keystore, null for no password;
     *                 this option will fail for non-archive distributions
     * @param daemonize Run Elasticsearch in the background
     * @param useTty Use a tty for inputting the password rather than standard input;
     *               this option will fail for non-archive distributions
     * @return Shell results of the startup command.
     * @throws Exception when command fails immediately.
     */
    public Shell.Result runElasticsearchStartCommand(String password, boolean daemonize, boolean useTty) throws Exception {
        if (password != null) {
            assertTrue("Only archives support user-entered passwords", distribution().isArchive());
        }

        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                if (useTty) {
                    return Archives.startElasticsearchWithTty(installation, sh, password, List.of(), null, daemonize);
                } else {
                    return Archives.runElasticsearchStartCommand(installation, sh, password, List.of(), daemonize);
                }
            case DEB:
            case RPM:
                return Packages.runElasticsearchStartCommand(sh);
            case DOCKER:
            case DOCKER_UBI:
            case DOCKER_IRON_BANK:
            case DOCKER_CLOUD:
            case DOCKER_CLOUD_ESS:
            case DOCKER_WOLFI:
                // nothing, "installing" docker image is running it
                return Shell.NO_OP;
            default:
                throw new IllegalStateException("Unknown Elasticsearch packaging type.");
        }
    }

    public void stopElasticsearch() throws Exception {
        switch (distribution.packaging) {
            case TAR:
            case ZIP:
                Archives.stopElasticsearch(installation);
                break;
            case DEB:
            case RPM:
                Packages.stopElasticsearch(sh);
                break;
            case DOCKER:
            case DOCKER_UBI:
            case DOCKER_IRON_BANK:
            case DOCKER_CLOUD:
            case DOCKER_CLOUD_ESS:
            case DOCKER_WOLFI:
                // nothing, "installing" docker image is running it
                break;
            default:
                throw new IllegalStateException("Unknown Elasticsearch packaging type.");
        }
    }

    public void awaitElasticsearchStartup(Shell.Result result) throws Exception {
        assertThat("Startup command should succeed. Stderr: [" + result + "]", result.exitCode(), equalTo(0));
        switch (distribution.packaging) {
            case TAR, ZIP -> Archives.assertElasticsearchStarted(installation);
            case DEB, RPM -> Packages.assertElasticsearchStarted(sh, installation);
            case DOCKER, DOCKER_UBI, DOCKER_IRON_BANK, DOCKER_CLOUD, DOCKER_CLOUD_ESS, DOCKER_WOLFI -> Docker.waitForElasticsearchToStart();
            default -> throw new IllegalStateException("Unknown Elasticsearch packaging type.");
        }
    }

    /**
     * Call {@link PackagingTestCase#awaitElasticsearchStartup}
     * returning the result.
     */
    public Shell.Result awaitElasticsearchStartupWithResult(Shell.Result result) throws Exception {
        awaitElasticsearchStartup(result);
        return result;
    }

    /**
     * Start Elasticsearch and wait until it's up and running. If you just want to run
     * the start command, use {@link #runElasticsearchStartCommand(String, boolean, boolean)}.
     * @throws Exception if Elasticsearch can't start
     */
    public void startElasticsearch() throws Exception {
        try {
            awaitElasticsearchStartup(runElasticsearchStartCommand(null, true, false));
        } catch (AssertionError | Exception e) {
            dumpDebug();
            throw e;
        }
    }

    public void assertElasticsearchFailure(Shell.Result result, String expectedMessage, Packages.JournaldWrapper journaldWrapper) {
        assertElasticsearchFailure(result, Collections.singletonList(expectedMessage), journaldWrapper);
    }

    public void assertElasticsearchFailure(Shell.Result result, List<String> expectedMessages, Packages.JournaldWrapper journaldWrapper) {
        @SuppressWarnings("unchecked")
        Matcher<String>[] stringMatchers = expectedMessages.stream().map(CoreMatchers::containsString).toArray(Matcher[]::new);
        if (Files.exists(installation.logs.resolve("elasticsearch.log"))) {

            // If log file exists, then we have bootstrapped our logging and the
            // error should be in the logs
            assertThat(installation.logs.resolve("elasticsearch.log"), fileExists());
            String logfile = FileUtils.slurp(installation.logs.resolve("elasticsearch.log"));
            if (logfile.isBlank()) {
                // bootstrap errors still
            }
            assertThat(logfile, anyOf(stringMatchers));

        } else if (distribution().isPackage() && Platforms.isSystemd()) {

            // For systemd, retrieve the error from journalctl
            assertThat(result.stderr(), containsString("Job for elasticsearch.service failed"));
            Shell.Result error = journaldWrapper.getLogs();
            assertThat(error.stdout(), anyOf(stringMatchers));

        } else {

            // Otherwise, error should be on shell stderr
            assertThat(result.stderr(), anyOf(stringMatchers));
        }
    }

    public void setFileSuperuser(String username, String password) {
        assertThat(installation, Matchers.not(Matchers.nullValue()));
        assertThat(fileSuperuserForInstallation, Matchers.nullValue());
        Shell.Result result = sh.run(
            installation.executables().usersTool + " useradd " + username + " -p " + password + " -r " + "superuser"
        );
        assertThat(result.isSuccess(), is(true));
        fileSuperuserForInstallation = new Tuple<>(username, password);
    }

    public void runElasticsearchTestsAsElastic(String elasticPassword) throws Exception {
        ServerUtils.runElasticsearchTests("elastic", elasticPassword, ServerUtils.getCaCert(installation));
    }

    public void runElasticsearchTests() throws Exception {
        ServerUtils.runElasticsearchTests(
            fileSuperuserForInstallation != null ? fileSuperuserForInstallation.v1() : null,
            fileSuperuserForInstallation != null ? fileSuperuserForInstallation.v2() : null,
            ServerUtils.getCaCert(installation)
        );
    }

    public String makeRequest(String request) throws Exception {
        return ServerUtils.makeRequest(
            Request.Get(request),
            fileSuperuserForInstallation != null ? fileSuperuserForInstallation.v1() : null,
            fileSuperuserForInstallation != null ? fileSuperuserForInstallation.v2() : null,
            ServerUtils.getCaCert(installation)
        );
    }

    public String makeRequestAsElastic(String request, String elasticPassword) throws Exception {
        return ServerUtils.makeRequest(Request.Get(request), "elastic", elasticPassword, ServerUtils.getCaCert(installation));
    }

    public int makeRequestAsElastic(String elasticPassword) throws Exception {
        return ServerUtils.makeRequestAndGetStatus(
            Request.Get("https://localhost:9200"),
            "elastic",
            elasticPassword,
            ServerUtils.getCaCert(installation)
        );
    }

    public static Path getRootTempDir() {
        if (distribution().isPackage()) {
            // The custom config directory is not under /tmp or /var/tmp because
            // systemd's private temp directory functionally means different
            // processes can have different views of what's in these directories
            return Paths.get("/var/test-tmp").toAbsolutePath();
        } else {
            // vagrant creates /tmp for us in windows so we use that to avoid long paths
            return Paths.get("/tmp").toAbsolutePath();
        }
    }

    private static final FileAttribute<?>[] NEW_DIR_PERMS;
    static {
        if (Platforms.WINDOWS) {
            NEW_DIR_PERMS = new FileAttribute<?>[0];
        } else {
            NEW_DIR_PERMS = new FileAttribute<?>[] { PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x")) };
        }
    }

    public static Path createTempDir(String prefix) throws IOException {
        return Files.createTempDirectory(getRootTempDir(), prefix, NEW_DIR_PERMS);
    }

    /**
     * Run the given action with a temporary copy of the config directory.
     *
     * Files under the path passed to the action may be modified as necessary for the
     * test to execute, and running Elasticsearch with {@link #startElasticsearch()} will
     * use the temporary directory.
     */
    public void withCustomConfig(CheckedConsumer<Path, Exception> action) throws Exception {
        Path tempDir = createTempDir("custom-config");
        Path tempConf = tempDir.resolve("elasticsearch");
        FileUtils.copyDirectory(installation.config, tempConf);

        // this is what install does
        sh.chown(tempDir);

        if (distribution.isPackage()) {
            Files.copy(installation.envFile, tempDir.resolve("elasticsearch.bk"), StandardCopyOption.COPY_ATTRIBUTES);// backup
            append(installation.envFile, "ES_PATH_CONF=" + tempConf + "\n");
        } else {
            sh.getEnv().put("ES_PATH_CONF", tempConf.toString());
        }

        // Auto-configuration file paths are absolute so we need to replace them in the config now that we copied them to tempConf
        // if auto-configuration has happened. Otherwise, the action below is a no-op.
        Path yml = tempConf.resolve("elasticsearch.yml");
        List<String> lines;
        try (Stream<String> allLines = Files.readAllLines(yml).stream()) {
            lines = allLines.map(l -> {
                if (l.contains(installation.config.toString())) {
                    return l.replace(installation.config.toString(), tempConf.toString());
                }
                return l;
            }).collect(Collectors.toList());
        }
        Files.write(yml, lines, TRUNCATE_EXISTING);
        action.accept(tempConf);
        if (distribution.isPackage()) {
            IOUtils.rm(installation.envFile);
            Files.copy(tempDir.resolve("elasticsearch.bk"), installation.envFile, StandardCopyOption.COPY_ATTRIBUTES);
        } else {
            sh.getEnv().remove("ES_PATH_CONF");
        }
        IOUtils.rm(tempDir);
    }

    public void withCustomConfigOwner(String tempOwner, Predicate<Distribution.Platform> predicate, CheckedRunnable<Exception> action)
        throws Exception {
        if (predicate.test(installation.distribution.platform)) {
            sh.chown(installation.config, tempOwner);
            action.run();
            sh.chown(installation.config);
        } else {
            action.run();
        }
    }

    /**
     * Manually set the heap size with a jvm.options.d file. This will be reset before each test.
     */
    public static void setHeap(String heapSize) throws IOException {
        setHeap(heapSize, installation.config);
    }

    public static void setHeap(String heapSize, Path config) throws IOException {
        Path heapOptions = config.resolve("jvm.options.d").resolve("heap.options");
        if (heapSize == null) {
            FileUtils.rm(heapOptions);
        } else {
            Files.writeString(
                heapOptions,
                String.format(Locale.ROOT, "-Xmx%1$s%n-Xms%1$s%n", heapSize),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
            );
        }
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
     * Validates that the installation {@code es} has been auto-configured. This applies to archives and docker only,
     * packages have nuances that justify their own version.
     * @param es the {@link Installation} to check
     */
    public void verifySecurityAutoConfigured(Installation es) throws Exception {
        final String autoConfigDirName = "certs";
        final Settings settings;
        if (es.distribution.isArchive()) {
            // We chown the installation on Windows to Administrators so that we can auto-configure it.
            String owner = Platforms.WINDOWS ? "BUILTIN\\Administrators" : "elasticsearch";
            assertThat(es.config(autoConfigDirName), FileMatcher.file(Directory, owner, owner, p750));
            Stream.of("http.p12", "http_ca.crt", "transport.p12")
                .forEach(file -> assertThat(es.config(autoConfigDirName).resolve(file), FileMatcher.file(File, owner, owner, p660)));
            settings = Settings.builder().loadFromPath(es.config("elasticsearch.yml")).build();
        } else if (es.distribution.isDocker()) {
            assertThat(es.config(autoConfigDirName), DockerFileMatcher.file(Directory, "elasticsearch", "root", p750));
            Stream.of("http.p12", "http_ca.crt", "transport.p12")
                .forEach(
                    file -> assertThat(
                        es.config(autoConfigDirName).resolve(file),
                        DockerFileMatcher.file(File, "elasticsearch", "root", p660)
                    )
                );
            Path localTempDir = createTempDir("docker-config");
            copyFromContainer(es.config("elasticsearch.yml"), localTempDir.resolve("docker_elasticsearch.yml"));
            settings = Settings.builder().loadFromPath(localTempDir.resolve("docker_elasticsearch.yml")).build();
            rm(localTempDir.resolve("docker_elasticsearch.yml"));
            rm(localTempDir);
        } else {
            assert es.distribution.isPackage();
            assertThat(es.config(autoConfigDirName), FileMatcher.file(Directory, "root", "elasticsearch", p750));
            Stream.of("http.p12", "http_ca.crt", "transport.p12")
                .forEach(
                    file -> assertThat(es.config(autoConfigDirName).resolve(file), FileMatcher.file(File, "root", "elasticsearch", p660))
                );
            assertThat(
                sh.run(es.executables().keystoreTool + " list").stdout(),
                Matchers.containsString("autoconfiguration.password_hash")
            );
            settings = Settings.builder().loadFromPath(es.config("elasticsearch.yml")).build();
        }
        assertThat(settings.get("xpack.security.enabled"), equalTo("true"));
        assertThat(settings.get("xpack.security.enrollment.enabled"), equalTo("true"));
        assertThat(settings.get("xpack.security.transport.ssl.enabled"), equalTo("true"));
        assertThat(settings.get("xpack.security.transport.ssl.verification_mode"), equalTo("certificate"));
        assertThat(settings.get("xpack.security.http.ssl.enabled"), equalTo("true"));
        assertThat(settings.get("xpack.security.enabled"), equalTo("true"));

        if (es.distribution.isDocker() == false) {
            assertThat(settings.get("http.host"), equalTo("0.0.0.0"));
        }
    }

    /**
     * Validates that the installation {@code es} has not been auto-configured. This applies to archives and docker only,
     * packages have nuances that justify their own version.
     * @param es the {@link Installation} to check
     */
    public static void verifySecurityNotAutoConfigured(Installation es) throws Exception {
        assertThat(Files.exists(es.config("certs")), Matchers.is(false));
        if (es.distribution.isPackage()) {
            if (Files.exists(es.config("elasticsearch.keystore"))) {
                assertThat(
                    sh.run(es.executables().keystoreTool + " list").stdout(),
                    not(Matchers.containsString("autoconfiguration.password_hash"))
                );
            }
        }
        List<String> configLines = Files.readAllLines(es.config("elasticsearch.yml"));
        assertThat(
            configLines,
            not(contains(containsString("#----------------------- BEGIN SECURITY AUTO CONFIGURATION -----------------------")))
        );
        Path caCert = ServerUtils.getCaCert(installation);
        if (caCert != null) {
            assertThat(caCert.toString(), Matchers.not(Matchers.containsString("certs")));
        }
    }
}
