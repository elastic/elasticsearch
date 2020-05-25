package org.elasticsearch.gradle.info;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.util.Util;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.internal.jvm.Jvm;
import org.gradle.jvm.toolchain.JavaInstallation;
import org.gradle.jvm.toolchain.JavaInstallationRegistry;
import org.gradle.util.GradleVersion;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(GlobalBuildInfoPlugin.class);
    private static final String DEFAULT_VERSION_JAVA_FILE_PATH = "server/src/main/java/org/elasticsearch/Version.java";
    private static Integer _defaultParallel = null;

    private final JavaInstallationRegistry javaInstallationRegistry;
    private final ObjectFactory objects;
    private final ProviderFactory providers;

    @Inject
    public GlobalBuildInfoPlugin(JavaInstallationRegistry javaInstallationRegistry, ObjectFactory objects, ProviderFactory providers) {
        this.javaInstallationRegistry = javaInstallationRegistry;
        this.objects = objects;
        this.providers = providers;
    }

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }

        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(Util.getResourceContents("/minimumCompilerVersion"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(Util.getResourceContents("/minimumRuntimeVersion"));

        File compilerJavaHome = findCompilerJavaHome();
        File runtimeJavaHome = findRuntimeJavaHome(compilerJavaHome);

        File rootDir = project.getRootDir();
        GitInfo gitInfo = gitInfo(rootDir);

        BuildParams.init(params -> {
            // Initialize global build parameters
            boolean isInternal = GlobalBuildInfoPlugin.class.getResource("/buildSrc.marker") != null;

            params.reset();
            params.setCompilerJavaHome(compilerJavaHome);
            params.setRuntimeJavaHome(runtimeJavaHome);
            params.setCompilerJavaVersion(determineJavaVersion("compiler java.home", compilerJavaHome, minimumCompilerVersion));
            params.setRuntimeJavaVersion(determineJavaVersion("runtime java.home", runtimeJavaHome, minimumRuntimeVersion));
            params.setIsRutimeJavaHomeSet(compilerJavaHome.equals(runtimeJavaHome) == false);
            params.setJavaVersions(getAvailableJavaVersions(minimumCompilerVersion));
            params.setMinimumCompilerVersion(minimumCompilerVersion);
            params.setMinimumRuntimeVersion(minimumRuntimeVersion);
            params.setGradleJavaVersion(Jvm.current().getJavaVersion());
            params.setGitRevision(gitInfo.getRevision());
            params.setGitOrigin(gitInfo.getOrigin());
            params.setBuildDate(ZonedDateTime.now(ZoneOffset.UTC));
            params.setTestSeed(getTestSeed());
            params.setIsCi(System.getenv("JENKINS_URL") != null);
            params.setIsInternal(isInternal);
            params.setDefaultParallel(findDefaultParallel(project));
            params.setInFipsJvm(Util.getBooleanProperty("tests.fips.enabled", false));
            params.setIsSnapshotBuild(Util.getBooleanProperty("build.snapshot", true));
            if (isInternal) {
                params.setBwcVersions(resolveBwcVersions(rootDir));
            }
        });

        // Print global build info header just before task execution
        project.getGradle().getTaskGraph().whenReady(graph -> logGlobalBuildInfo());
    }

    /* Introspect all versions of ES that may be tested against for backwards
     * compatibility. It is *super* important that this logic is the same as the
     * logic in VersionUtils.java. */
    private static BwcVersions resolveBwcVersions(File root) {
        File versionsFile = new File(root, DEFAULT_VERSION_JAVA_FILE_PATH);
        try {
            List<String> versionLines = IOUtils.readLines(new FileInputStream(versionsFile), "UTF-8");
            return new BwcVersions(versionLines);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to resolve to resolve bwc versions from versionsFile.", e);
        }
    }

    private void logGlobalBuildInfo() {
        final String osName = System.getProperty("os.name");
        final String osVersion = System.getProperty("os.version");
        final String osArch = System.getProperty("os.arch");
        final Jvm gradleJvm = Jvm.current();
        final String gradleJvmDetails = getJavaInstallation(gradleJvm.getJavaHome()).getImplementationName();

        LOGGER.quiet("=======================================");
        LOGGER.quiet("Elasticsearch Build Hamster says Hello!");
        LOGGER.quiet("  Gradle Version        : " + GradleVersion.current().getVersion());
        LOGGER.quiet("  OS Info               : " + osName + " " + osVersion + " (" + osArch + ")");
        if (Jvm.current().getJavaVersion().equals(BuildParams.getCompilerJavaVersion()) == false || BuildParams.getIsRuntimeJavaHomeSet()) {
            String compilerJvmDetails = getJavaInstallation(BuildParams.getCompilerJavaHome()).getImplementationName();
            String runtimeJvmDetails = getJavaInstallation(BuildParams.getRuntimeJavaHome()).getImplementationName();

            LOGGER.quiet("  Compiler JDK Version  : " + BuildParams.getCompilerJavaVersion() + " (" + compilerJvmDetails + ")");
            LOGGER.quiet("  Compiler java.home    : " + BuildParams.getCompilerJavaHome());
            LOGGER.quiet("  Runtime JDK Version   : " + BuildParams.getRuntimeJavaVersion() + " (" + runtimeJvmDetails + ")");
            LOGGER.quiet("  Runtime java.home     : " + BuildParams.getRuntimeJavaHome());
            LOGGER.quiet("  Gradle JDK Version    : " + gradleJvm.getJavaVersion() + " (" + gradleJvmDetails + ")");
            LOGGER.quiet("  Gradle java.home      : " + gradleJvm.getJavaHome());
        } else {
            LOGGER.quiet("  JDK Version           : " + gradleJvm.getJavaVersion() + " (" + gradleJvmDetails + ")");
            LOGGER.quiet("  JAVA_HOME             : " + gradleJvm.getJavaHome());
        }
        LOGGER.quiet("  Random Testing Seed   : " + BuildParams.getTestSeed());
        LOGGER.quiet("  In FIPS 140 mode      : " + BuildParams.isInFipsJvm());
        LOGGER.quiet("=======================================");
    }

    private JavaVersion determineJavaVersion(String description, File javaHome, JavaVersion requiredVersion) {
        JavaInstallation installation = getJavaInstallation(javaHome);
        JavaVersion actualVersion = installation.getJavaVersion();
        if (actualVersion.isCompatibleWith(requiredVersion) == false) {
            throwInvalidJavaHomeException(
                description,
                javaHome,
                Integer.parseInt(requiredVersion.getMajorVersion()),
                Integer.parseInt(actualVersion.getMajorVersion())
            );
        }

        return actualVersion;
    }

    private JavaInstallation getJavaInstallation(File javaHome) {
        JavaInstallation installation;
        if (isCurrentJavaHome(javaHome)) {
            installation = javaInstallationRegistry.getInstallationForCurrentVirtualMachine().get();
        } else {
            installation = javaInstallationRegistry.installationForDirectory(objects.directoryProperty().fileValue(javaHome)).get();
        }

        return installation;
    }

    private List<JavaHome> getAvailableJavaVersions(JavaVersion minimumCompilerVersion) {
        final List<JavaHome> javaVersions = new ArrayList<>();
        for (int v = 8; v <= Integer.parseInt(minimumCompilerVersion.getMajorVersion()); v++) {
            int version = v;
            String javaHomeEnvVarName = getJavaHomeEnvVarName(Integer.toString(version));
            if (System.getenv(javaHomeEnvVarName) != null) {
                File javaHomeDirectory = new File(findJavaHome(Integer.toString(version)));
                Provider<JavaInstallation> javaInstallationProvider = javaInstallationRegistry.installationForDirectory(
                    objects.directoryProperty().fileValue(javaHomeDirectory)
                );
                JavaHome javaHome = JavaHome.of(version, providers.provider(() -> {
                    int actualVersion = Integer.parseInt(javaInstallationProvider.get().getJavaVersion().getMajorVersion());
                    if (actualVersion != version) {
                        throwInvalidJavaHomeException("env variable " + javaHomeEnvVarName, javaHomeDirectory, version, actualVersion);
                    }
                    return javaHomeDirectory;
                }));
                javaVersions.add(javaHome);
            }
        }
        return javaVersions;
    }

    private static boolean isCurrentJavaHome(File javaHome) {
        try {
            return Files.isSameFile(javaHome.toPath(), Jvm.current().getJavaHome().toPath());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String getTestSeed() {
        String testSeedProperty = System.getProperty("tests.seed");
        final String testSeed;
        if (testSeedProperty == null) {
            long seed = new Random(System.currentTimeMillis()).nextLong();
            testSeed = Long.toUnsignedString(seed, 16).toUpperCase(Locale.ROOT);
        } else {
            testSeed = testSeedProperty;
        }
        return testSeed;
    }

    private static void throwInvalidJavaHomeException(String description, File javaHome, int expectedVersion, int actualVersion) {
        String message = String.format(
            Locale.ROOT,
            "The %s must be set to a JDK installation directory for Java %d but is [%s] corresponding to [%s]",
            description,
            expectedVersion,
            javaHome,
            actualVersion
        );

        throw new GradleException(message);
    }

    private static File findCompilerJavaHome() {
        String compilerJavaHome = System.getenv("JAVA_HOME");
        String compilerJavaProperty = System.getProperty("compiler.java");

        if (compilerJavaProperty != null) {
            compilerJavaHome = findJavaHome(compilerJavaProperty);
        }

        // if JAVA_HOME is not set,so we use the JDK that Gradle was run with.
        return compilerJavaHome == null ? Jvm.current().getJavaHome() : new File(compilerJavaHome);
    }

    private static File findRuntimeJavaHome(final File compilerJavaHome) {
        String runtimeJavaProperty = System.getProperty("runtime.java");

        if (runtimeJavaProperty != null) {
            return new File(findJavaHome(runtimeJavaProperty));
        }

        return System.getenv("RUNTIME_JAVA_HOME") == null ? compilerJavaHome : new File(System.getenv("RUNTIME_JAVA_HOME"));
    }

    private static String findJavaHome(String version) {
        String versionedJavaHome = System.getenv(getJavaHomeEnvVarName(version));
        if (versionedJavaHome == null) {
            final String exceptionMessage = String.format(
                Locale.ROOT,
                "$%s must be set to build Elasticsearch. "
                    + "Note that if the variable was just set you "
                    + "might have to run `./gradlew --stop` for "
                    + "it to be picked up. See https://github.com/elastic/elasticsearch/issues/31399 details.",
                getJavaHomeEnvVarName(version)
            );

            throw new GradleException(exceptionMessage);
        }
        return versionedJavaHome;
    }

    private static String getJavaHomeEnvVarName(String version) {
        return "JAVA" + version + "_HOME";
    }

    private static int findDefaultParallel(Project project) {
        // Since it costs IO to compute this, and is done at configuration time we want to cache this if possible
        // It's safe to store this in a static variable since it's just a primitive so leaking memory isn't an issue
        if (_defaultParallel == null) {
            File cpuInfoFile = new File("/proc/cpuinfo");
            if (cpuInfoFile.exists()) {
                // Count physical cores on any Linux distro ( don't count hyper-threading )
                Map<String, Integer> socketToCore = new HashMap<>();
                String currentID = "";

                try (BufferedReader reader = new BufferedReader(new FileReader(cpuInfoFile))) {
                    for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                        if (line.contains(":")) {
                            List<String> parts = Arrays.stream(line.split(":", 2)).map(String::trim).collect(Collectors.toList());
                            String name = parts.get(0);
                            String value = parts.get(1);
                            // the ID of the CPU socket
                            if (name.equals("physical id")) {
                                currentID = value;
                            }
                            // Number of cores not including hyper-threading
                            if (name.equals("cpu cores")) {
                                assert currentID.isEmpty() == false;
                                socketToCore.put("currentID", Integer.valueOf(value));
                                currentID = "";
                            }
                        }
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                _defaultParallel = socketToCore.values().stream().mapToInt(i -> i).sum();
            } else if (OS.current() == OS.MAC) {
                // Ask macOS to count physical CPUs for us
                ByteArrayOutputStream stdout = new ByteArrayOutputStream();
                project.exec(spec -> {
                    spec.setExecutable("sysctl");
                    spec.args("-n", "hw.physicalcpu");
                    spec.setStandardOutput(stdout);
                });

                _defaultParallel = Integer.parseInt(stdout.toString().trim());
            }

            _defaultParallel = Runtime.getRuntime().availableProcessors() / 2;
        }

        return _defaultParallel;
    }

    public static GitInfo gitInfo(File rootDir) {
        try {
            /*
             * We want to avoid forking another process to run git rev-parse HEAD. Instead, we will read the refs manually. The
             * documentation for this follows from https://git-scm.com/docs/gitrepository-layout and https://git-scm.com/docs/git-worktree.
             *
             * There are two cases to consider:
             *  - a plain repository with .git directory at the root of the working tree
             *  - a worktree with a plain text .git file at the root of the working tree
             *
             * In each case, our goal is to parse the HEAD file to get either a ref or a bare revision (in the case of being in detached
             * HEAD state).
             *
             * In the case of a plain repository, we can read the HEAD file directly, resolved directly from the .git directory.
             *
             * In the case of a worktree, we read the gitdir from the plain text .git file. This resolves to a directory from which we read
             * the HEAD file and resolve commondir to the plain git repository.
             */
            final Path dotGit = rootDir.toPath().resolve(".git");
            final String revision;
            if (Files.exists(dotGit) == false) {
                return new GitInfo("unknown", "unknown");
            }
            final Path head;
            final Path gitDir;
            if (Files.isDirectory(dotGit)) {
                // this is a git repository, we can read HEAD directly
                head = dotGit.resolve("HEAD");
                gitDir = dotGit;
            } else {
                // this is a git worktree, follow the pointer to the repository
                final Path workTree = Paths.get(readFirstLine(dotGit).substring("gitdir:".length()).trim());
                if (Files.exists(workTree) == false) {
                    return new GitInfo("unknown", "unknown");
                }
                head = workTree.resolve("HEAD");
                final Path commonDir = Paths.get(readFirstLine(workTree.resolve("commondir")));
                if (commonDir.isAbsolute()) {
                    gitDir = commonDir;
                } else {
                    // this is the common case
                    gitDir = workTree.resolve(commonDir);
                }
            }
            final String ref = readFirstLine(head);
            if (ref.startsWith("ref:")) {
                String refName = ref.substring("ref:".length()).trim();
                Path refFile = gitDir.resolve(refName);
                if (Files.exists(refFile)) {
                    revision = readFirstLine(refFile);
                } else if (Files.exists(gitDir.resolve("packed-refs"))) {
                    // Check packed references for commit ID
                    Pattern p = Pattern.compile("^([a-f0-9]{40}) " + refName + "$");
                    try (Stream<String> lines = Files.lines(gitDir.resolve("packed-refs"))) {
                        revision = lines.map(p::matcher)
                            .filter(Matcher::matches)
                            .map(m -> m.group(1))
                            .findFirst()
                            .orElseThrow(() -> new IOException("Packed reference not found for refName " + refName));
                    }
                } else {
                    throw new GradleException("Can't find revision for refName " + refName);
                }
            } else {
                // we are in detached HEAD state
                revision = ref;
            }
            return new GitInfo(revision, findOriginUrl(gitDir.resolve("config")));
        } catch (final IOException e) {
            // for now, do not be lenient until we have better understanding of real-world scenarios where this happens
            throw new GradleException("unable to read the git revision", e);
        }
    }

    private static String findOriginUrl(final Path configFile) throws IOException {
        Map<String, String> props = new HashMap<>();

        try (Stream<String> stream = Files.lines(configFile, StandardCharsets.UTF_8)) {
            Iterator<String> lines = stream.iterator();
            boolean foundOrigin = false;
            while (lines.hasNext()) {
                String line = lines.next().trim();
                if (line.startsWith(";") || line.startsWith("#")) {
                    // ignore comments
                    continue;
                }
                if (foundOrigin) {
                    if (line.startsWith("[")) {
                        // we're on to the next config item so stop looking
                        break;
                    }
                    String[] pair = line.trim().split("=");
                    props.put(pair[0].trim(), pair[1].trim());
                } else {
                    if (line.equals("[remote \"origin\"]")) {
                        foundOrigin = true;
                    }
                }
            }
        }

        String originUrl = props.get("url");
        return originUrl == null ? "unknown" : originUrl;
    }

    private static String readFirstLine(final Path path) throws IOException {
        String firstLine;
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            firstLine = lines.findFirst().orElseThrow(() -> new IOException("file [" + path + "] is empty"));
        }
        return firstLine;
    }

    private static class GitInfo {
        private final String revision;
        private final String origin;

        GitInfo(String revision, String origin) {
            this.revision = revision;
            this.origin = origin;
        }

        public String getRevision() {
            return revision;
        }

        public String getOrigin() {
            return origin;
        }
    }
}
