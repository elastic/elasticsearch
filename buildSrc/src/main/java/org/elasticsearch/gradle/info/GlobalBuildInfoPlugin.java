package org.elasticsearch.gradle.info;

import org.elasticsearch.gradle.OS;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.internal.jvm.Jvm;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final String GLOBAL_INFO_EXTENSION_NAME = "globalInfo";
    private static Integer _defaultParallel = null;

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }

        GlobalInfoExtension extension = project.getExtensions().create(GLOBAL_INFO_EXTENSION_NAME, GlobalInfoExtension.class);

        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(getResourceContents("/minimumCompilerVersion"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(getResourceContents("/minimumRuntimeVersion"));

        File compilerJavaHome = findCompilerJavaHome();
        File runtimeJavaHome = findRuntimeJavaHome(compilerJavaHome);

        String testSeedProperty = System.getProperty("tests.seed");
        final String testSeed;
        if (testSeedProperty == null) {
            long seed = new Random(System.currentTimeMillis()).nextLong();
            testSeed = Long.toUnsignedString(seed, 16).toUpperCase(Locale.ROOT);
        } else {
            testSeed = testSeedProperty;
        }

        final List<JavaHome> javaVersions = new ArrayList<>();
        for (int version = 8; version <= Integer.parseInt(minimumCompilerVersion.getMajorVersion()); version++) {
            if (System.getenv(getJavaHomeEnvVarName(Integer.toString(version))) != null) {
                javaVersions.add(JavaHome.of(version, new File(findJavaHome(Integer.toString(version)))));
            }
        }

        GenerateGlobalBuildInfoTask generateTask = project.getTasks()
            .create("generateGlobalBuildInfo", GenerateGlobalBuildInfoTask.class, task -> {
                task.setJavaVersions(javaVersions);
                task.setMinimumCompilerVersion(minimumCompilerVersion);
                task.setMinimumRuntimeVersion(minimumRuntimeVersion);
                task.setCompilerJavaHome(compilerJavaHome);
                task.setRuntimeJavaHome(runtimeJavaHome);
                task.getOutputFile().set(new File(project.getBuildDir(), "global-build-info"));
                task.getCompilerVersionFile().set(new File(project.getBuildDir(), "java-compiler-version"));
                task.getRuntimeVersionFile().set(new File(project.getBuildDir(), "java-runtime-version"));
            });

        PrintGlobalBuildInfoTask printTask = project.getTasks().create("printGlobalBuildInfo", PrintGlobalBuildInfoTask.class, task -> {
            task.getBuildInfoFile().set(generateTask.getOutputFile());
            task.getCompilerVersionFile().set(generateTask.getCompilerVersionFile());
            task.getRuntimeVersionFile().set(generateTask.getRuntimeVersionFile());
            task.setGlobalInfoListeners(extension.listeners);
        });

        // Initialize global build parameters
        BuildParams.init(params -> {
            params.reset();
            params.setCompilerJavaHome(compilerJavaHome);
            params.setRuntimeJavaHome(runtimeJavaHome);
            params.setIsRutimeJavaHomeSet(compilerJavaHome.equals(runtimeJavaHome) == false);
            params.setJavaVersions(javaVersions);
            params.setMinimumCompilerVersion(minimumCompilerVersion);
            params.setMinimumRuntimeVersion(minimumRuntimeVersion);
            params.setGradleJavaVersion(Jvm.current().getJavaVersion());
            params.setGitRevision(gitRevision(project.getRootProject().getRootDir()));
            params.setBuildDate(ZonedDateTime.now(ZoneOffset.UTC));
            params.setTestSeed(testSeed);
            params.setIsCi(System.getenv("JENKINS_URL") != null);
            params.setIsInternal(GlobalBuildInfoPlugin.class.getResource("/buildSrc.marker") != null);
            params.setDefaultParallel(findDefaultParallel(project));
            params.setInFipsJvm(isInFipsJvm());
        });

        project.allprojects(
            p -> {
                // Make sure than any task execution generates and prints build info
                p.getTasks().configureEach(task -> {
                    if (task != generateTask && task != printTask) {
                        task.dependsOn(printTask);
                    }
                });
            }
        );
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

    private static boolean isInFipsJvm() {
        return Boolean.parseBoolean(System.getProperty("tests.fips.enabled"));
    }

    private static String getResourceContents(String resourcePath) {
        try (
            BufferedReader reader = new BufferedReader(new InputStreamReader(GlobalBuildInfoPlugin.class.getResourceAsStream(resourcePath)))
        ) {
            StringBuilder b = new StringBuilder();
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                if (b.length() != 0) {
                    b.append('\n');
                }
                b.append(line);
            }

            return b.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Error trying to read classpath resource: " + resourcePath, e);
        }
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

    public static String gitRevision(File rootDir) {
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
                return "unknown";
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
            return revision;
        } catch (final IOException e) {
            // for now, do not be lenient until we have better understanding of real-world scenarios where this happens
            throw new GradleException("unable to read the git revision", e);
        }
    }

    private static String readFirstLine(final Path path) throws IOException {
        String firstLine;
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            firstLine = lines.findFirst().orElseThrow(() -> new IOException("file [" + path + "] is empty"));
        }
        return firstLine;
    }
}
