/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.precommit;

import de.thetaphi.forbiddenapis.cli.CliMain;

import org.apache.commons.io.output.NullOutputStream;
import org.elasticsearch.gradle.OS;
import org.gradle.api.DefaultTask;
import org.gradle.api.JavaVersion;
import org.gradle.api.file.ArchiveOperations;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;

@CacheableTask
public abstract class ThirdPartyAuditTask extends DefaultTask {

    private static final Pattern MISSING_CLASS_PATTERN = Pattern.compile(
        "WARNING: Class '(.*)' cannot be loaded \\(.*\\)\\. Please fix the classpath!"
    );

    private static final Pattern VIOLATION_PATTERN = Pattern.compile("\\s\\sin ([a-zA-Z0-9$.]+) \\(.*\\)");
    private static final int SIG_KILL_EXIT_VALUE = 137;
    private static final List<Integer> EXPECTED_EXIT_CODES = Arrays.asList(
        CliMain.EXIT_SUCCESS,
        CliMain.EXIT_VIOLATION,
        CliMain.EXIT_UNSUPPORTED_JDK
    );
    private static final String JDK_JAR_HELL_MAIN_CLASS = "org.elasticsearch.jdk.JdkJarHellCheck";

    private Set<String> missingClassExcludes = new TreeSet<>();

    private Set<String> violationsExcludes = new TreeSet<>();

    private Set<String> jdkJarHellExcludes = new TreeSet<>();

    private File signatureFile;

    private Property<String> javaHome;

    private final Property<JavaVersion> targetCompatibility;

    private final ArchiveOperations archiveOperations;

    private final ExecOperations execOperations;

    private final FileSystemOperations fileSystemOperations;

    private final ProjectLayout projectLayout;

    private FileCollection classpath;

    @Inject
    public ThirdPartyAuditTask(
        ArchiveOperations archiveOperations,
        ExecOperations execOperations,
        FileSystemOperations fileSystemOperations,
        ProjectLayout projectLayout,
        ObjectFactory objectFactory
    ) {
        this.archiveOperations = archiveOperations;
        this.execOperations = execOperations;
        this.fileSystemOperations = fileSystemOperations;
        this.projectLayout = projectLayout;
        this.targetCompatibility = objectFactory.property(JavaVersion.class);
        this.javaHome = objectFactory.property(String.class);
    }

    @Input
    public Property<JavaVersion> getTargetCompatibility() {
        return targetCompatibility;
    }

    @InputFiles
    @PathSensitive(PathSensitivity.NAME_ONLY)
    public abstract ConfigurableFileCollection getForbiddenAPIsClasspath();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public File getSignatureFile() {
        return signatureFile;
    }

    public void setSignatureFile(File signatureFile) {
        this.signatureFile = signatureFile;
    }

    @Internal
    public Property<String> getJavaHome() {
        return javaHome;
    }

    @Internal
    public File getJarExpandDir() {
        return projectLayout.getBuildDirectory().dir("precommit/thirdPartyAudit").get().dir(getName()).getAsFile();
    }

    @OutputFile
    public File getSuccessMarker() {
        return projectLayout.getBuildDirectory().dir("precommit/thirdPartyAudit").get().dir("markers/").file(getName()).getAsFile();
    }

    // We use compile classpath normalization here because class implementation changes are irrelevant for the purposes of jdk jar hell.
    // We only care about the runtime classpath ABI here.
    @CompileClasspath
    abstract ConfigurableFileCollection getJdkJarHellClasspath();

    public void ignoreMissingClasses(String... classesOrPackages) {
        if (classesOrPackages.length == 0) {
            missingClassExcludes = null;
            return;
        }
        if (missingClassExcludes == null) {
            missingClassExcludes = new TreeSet<>();
        }
        for (String each : classesOrPackages) {
            missingClassExcludes.add(each);
        }
    }

    public void ignoreViolations(String... violatingClasses) {
        for (String each : violatingClasses) {
            violationsExcludes.add(each);
        }
    }

    public void ignoreJarHellWithJDK(String... classes) {
        for (String each : classes) {
            jdkJarHellExcludes.add(each);
        }
    }

    @Input
    public Set<String> getJdkJarHellExcludes() {
        return jdkJarHellExcludes;
    }

    @Input
    @Optional
    public Set<String> getMissingClassExcludes() {
        return missingClassExcludes;
    }

    @Classpath
    @SkipWhenEmpty
    public abstract ConfigurableFileCollection getJarsToScan();

    @TaskAction
    public void runThirdPartyAudit() throws IOException {
        Set<File> jars = getJarsToScan().getFiles();
        extractJars(jars, getJarExpandDir());
        final String forbiddenApisOutput = runForbiddenAPIsCli();
        final Set<String> missingClasses = new TreeSet<>();
        Matcher missingMatcher = MISSING_CLASS_PATTERN.matcher(forbiddenApisOutput);
        while (missingMatcher.find()) {
            missingClasses.add(missingMatcher.group(1));
        }

        final Set<String> violationsClasses = new TreeSet<>();
        Matcher violationMatcher = VIOLATION_PATTERN.matcher(forbiddenApisOutput);
        while (violationMatcher.find()) {
            violationsClasses.add(violationMatcher.group(1));
        }

        Set<String> jdkJarHellClasses = runJdkJarHellCheck();

        if (missingClassExcludes != null) {
            long bogousExcludesCount = Stream.concat(missingClassExcludes.stream(), violationsExcludes.stream())
                .filter(each -> missingClasses.contains(each) == false)
                .filter(each -> violationsClasses.contains(each) == false)
                .count();
            if (bogousExcludesCount != 0 && bogousExcludesCount == missingClassExcludes.size() + violationsExcludes.size()) {
                logForbiddenAPIsOutput(forbiddenApisOutput);
                throw new IllegalStateException(
                    "All excluded classes seem to have no issues. " + "This is sometimes an indication that the check silently failed"
                );
            }
            assertNoPointlessExclusions("are not missing", missingClassExcludes, missingClasses);
            missingClasses.removeAll(missingClassExcludes);
        }
        assertNoPointlessExclusions("have no violations", violationsExcludes, violationsClasses);
        assertNoPointlessExclusions("do not generate jar hell with the JDK", jdkJarHellExcludes, jdkJarHellClasses);

        if (missingClassExcludes == null && (missingClasses.isEmpty() == false)) {
            getLogger().info("Found missing classes, but task is configured to ignore all of them:\n {}", formatClassList(missingClasses));
            missingClasses.clear();
        }

        violationsClasses.removeAll(violationsExcludes);
        if (missingClasses.isEmpty() && violationsClasses.isEmpty()) {
            getLogger().info("Third party audit passed successfully");
        } else {
            logForbiddenAPIsOutput(forbiddenApisOutput);
            if (missingClasses.isEmpty() == false) {
                getLogger().error("Missing classes:\n{}", formatClassList(missingClasses));
            }
            if (violationsClasses.isEmpty() == false) {
                getLogger().error("Classes with violations:\n{}", formatClassList(violationsClasses));
            }
            throw new IllegalStateException("Audit of third party dependencies failed");
        }

        assertNoJarHell(jdkJarHellClasses);

        success();
    }

    // Mark successful third party audit check
    private void success() throws IOException {
        getSuccessMarker().getParentFile().mkdirs();
        Files.write(getSuccessMarker().toPath(), new byte[] {});
    }

    private void logForbiddenAPIsOutput(String forbiddenApisOutput) {
        getLogger().error("Forbidden APIs output:\n{}==end of forbidden APIs==", forbiddenApisOutput);
    }

    private void throwNotConfiguredCorrectlyException() {
        throw new IllegalArgumentException("Audit of third party dependencies is not configured correctly");
    }

    /**
     * Ideally we would do unpacking already via artifact transform and keep unpacked jars across builds.
     * At the moment transform target folder is not configurable and forbidden CLI only takes one common
     * directory as input which makes it incompatible with gradle artifact transforms as we use them today.
     * */
    private void extractJars(Set<File> jars, File jarExpandDir) {
        // We need to clean up to make sure old dependencies don't linger
        fileSystemOperations.delete(d -> d.delete(jarExpandDir));

        jars.forEach(jar -> {
            FileTree jarFiles = archiveOperations.zipTree(jar);
            fileSystemOperations.copy(spec -> {
                spec.from(jarFiles);
                spec.into(jarExpandDir);
                // exclude classes from multi release jars
                spec.exclude("META-INF/versions/**");
            });
            // Deal with multi release jars:
            // The order is important, we iterate here so we don't depend on the order in which Gradle executes the spec
            // We extract multi release jar classes ( if these exist ) going from 9 - the first to support them, to the
            // current `targetCompatibility` version.
            // Each extract will overwrite the top level classes that existed before it, the result is that we end up
            // with a single version of the class in `jarExpandDir`.
            // This will be the closes version to `targetCompatibility`, the same class that would be loaded in a JVM
            // that has `targetCompatibility` version.
            // This means we only scan classes that would be loaded into `targetCompatibility`, and don't look at any
            // pther version specific implementation of said classes.
            IntStream.rangeClosed(
                Integer.parseInt(JavaVersion.VERSION_1_9.getMajorVersion()),
                Integer.parseInt(targetCompatibility.get().getMajorVersion())
            ).forEach(majorVersion -> fileSystemOperations.copy(spec -> {
                spec.from(archiveOperations.zipTree(jar));
                spec.into(jarExpandDir);
                String metaInfPrefix = "META-INF/versions/" + majorVersion;
                spec.include(metaInfPrefix + "/**");
                // Drop the version specific prefix
                spec.eachFile(details -> details.setPath(details.getPath().replace(metaInfPrefix, "")));
                spec.setIncludeEmptyDirs(false);
            }));
        });
    }

    private void assertNoJarHell(Set<String> jdkJarHellClasses) {
        jdkJarHellClasses.removeAll(jdkJarHellExcludes);
        if (jdkJarHellClasses.isEmpty() == false) {
            throw new IllegalStateException(
                "Audit of third party dependencies failed:\n" + "  Jar Hell with the JDK:\n" + formatClassList(jdkJarHellClasses)
            );
        }
    }

    private void assertNoPointlessExclusions(String specifics, Set<String> excludes, Set<String> problematic) {
        String notMissing = excludes.stream()
            .filter(each -> problematic.contains(each) == false)
            .map(each -> "  * " + each)
            .collect(Collectors.joining("\n"));
        if (notMissing.isEmpty() == false) {
            getLogger().error("Unnecessary exclusions, following classes " + specifics + ":\n {}", notMissing);
            throw new IllegalStateException("Third party audit task is not configured correctly");
        }
    }

    private String formatClassList(Set<String> classList) {
        return classList.stream().map(name -> "  * " + name).sorted().collect(Collectors.joining("\n"));
    }

    private String runForbiddenAPIsCli() throws IOException {
        ByteArrayOutputStream errorOut = new ByteArrayOutputStream();
        ExecResult result = execOperations.javaexec(spec -> {
            if (javaHome.isPresent()) {
                spec.setExecutable(javaHome.get() + "/bin/java");
            }
            spec.classpath(getForbiddenAPIsClasspath(), classpath);
            spec.jvmArgs("-Xmx1g");
            spec.getMainClass().set("de.thetaphi.forbiddenapis.cli.CliMain");
            spec.args("-f", getSignatureFile().getAbsolutePath(), "-d", getJarExpandDir(), "--allowmissingclasses");
            spec.setErrorOutput(errorOut);
            if (getLogger().isInfoEnabled() == false) {
                spec.setStandardOutput(new NullOutputStream());
            }
            spec.setIgnoreExitValue(true);
        });
        if (OS.current().equals(OS.LINUX) && result.getExitValue() == SIG_KILL_EXIT_VALUE) {
            throw new IllegalStateException("Third party audit was killed buy SIGKILL, could be a victim of the Linux OOM killer");
        }
        final String forbiddenApisOutput;
        try (ByteArrayOutputStream outputStream = errorOut) {
            forbiddenApisOutput = outputStream.toString(StandardCharsets.UTF_8.name());
        }
        if (EXPECTED_EXIT_CODES.contains(result.getExitValue()) == false) {
            throw new IllegalStateException("Forbidden APIs cli failed: " + forbiddenApisOutput);
        }
        return forbiddenApisOutput;
    }

    private Set<String> runJdkJarHellCheck() throws IOException {
        ByteArrayOutputStream standardOut = new ByteArrayOutputStream();
        ExecResult execResult = execOperations.javaexec(spec -> {
            spec.classpath(getJdkJarHellClasspath(), classpath);
            spec.getMainClass().set(JDK_JAR_HELL_MAIN_CLASS);
            spec.args(getJarExpandDir());
            spec.setIgnoreExitValue(true);
            if (javaHome.isPresent()) {
                spec.setExecutable(javaHome.get() + "/bin/java");
            }
            spec.setStandardOutput(standardOut);
        });
        if (execResult.getExitValue() == 0) {
            return Collections.emptySet();
        }
        final String jdkJarHellCheckList;
        try (ByteArrayOutputStream outputStream = standardOut) {
            jdkJarHellCheckList = outputStream.toString(StandardCharsets.UTF_8.name());
        }
        return new TreeSet<>(Arrays.asList(jdkJarHellCheckList.split("\\r?\\n")));
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

}
