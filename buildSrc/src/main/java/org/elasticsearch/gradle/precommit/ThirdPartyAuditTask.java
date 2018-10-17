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
package org.elasticsearch.gradle.precommit;

import org.apache.commons.io.output.NullOutputStream;
import org.elasticsearch.gradle.JdkJarHellCheck;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.StopExecutionException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecResult;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ThirdPartyAuditTask extends DefaultTask {

    private static final Pattern MISSING_CLASS_PATTERN = Pattern.compile(
        "WARNING: Class '(.*)' cannot be loaded \\(.*\\)\\. Please fix the classpath!"
    );

    private static final Pattern VIOLATION_PATTERN = Pattern.compile(
        "\\s\\sin ([a-zA-Z0-9$.]+) \\(.*\\)"
    );

    /**
     * patterns for classes to exclude, because we understand their issues
     */
    private Set<String> excludes = new TreeSet<>();

    private File signatureFile;

    private String javaHome;

    private JavaVersion targetCompatibility;

    @Input
    public JavaVersion getTargetCompatibility() {
        return targetCompatibility;
    }

    public void setTargetCompatibility(JavaVersion targetCompatibility) {
        this.targetCompatibility = targetCompatibility;
    }

    @InputFiles
    public Configuration getForbiddenAPIsConfiguration() {
        return getProject().getConfigurations().getByName("forbiddenApisCliJar");
    }

    @InputFile
    public File getSignatureFile() {
        return signatureFile;
    }

    public void setSignatureFile(File signatureFile) {
        this.signatureFile = signatureFile;
    }

    @InputFiles
    public Configuration getRuntimeConfiguration() {
        Configuration runtime = getProject().getConfigurations().findByName("runtime");
        if (runtime == null) {
            return getProject().getConfigurations().getByName("testCompile");
        }
        return runtime;
    }

    @Input
    public String getJavaHome() {
        return javaHome;
    }

    public void setJavaHome(String javaHome) {
        this.javaHome = javaHome;
    }

    @InputFiles
    public Configuration getCompileOnlyConfiguration() {
        return getProject().getConfigurations().getByName("compileOnly");
    }

    @OutputDirectory
    public File getJarExpandDir() {
        return new File(
            new File(getProject().getBuildDir(), "precommit/thirdPartyAudit"),
            getName()
        );
    }

    public void setExcludes(String... classes) {
        excludes.clear();
        for (String each : classes) {
            if (each.indexOf('*') != -1) {
                throw new IllegalArgumentException("illegal third party audit exclusion: '" + each + "', wildcards are not permitted!");
            }
            excludes.add(each);
        }
    }

    @Input
    public Set<String> getExcludes() {
        return Collections.unmodifiableSet(excludes);
    }

    @TaskAction
    public void runThirdPartyAudit() throws IOException {
        FileCollection jars = getJarsToScan();

        extractJars(jars);

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

        assertNoPointlessExclusions(missingClasses, violationsClasses, jdkJarHellClasses);

        assertNoMissingAndViolations(missingClasses, violationsClasses);

        assertNoJarHell(jdkJarHellClasses);
    }

    private void extractJars(FileCollection jars) {
        File jarExpandDir = getJarExpandDir();
        // We need to clean up to make sure old dependencies don't linger
        getProject().delete(jarExpandDir);

        jars.forEach(jar -> {
            FileTree jarFiles = getProject().zipTree(jar);
            getProject().copy(spec -> {
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
                Integer.parseInt(targetCompatibility.getMajorVersion())
            ).forEach(majorVersion -> getProject().copy(spec -> {
                spec.from(getProject().zipTree(jar));
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
        jdkJarHellClasses.removeAll(excludes);
        if (jdkJarHellClasses.isEmpty() == false) {
            throw new IllegalStateException("Jar Hell with the JDK:" + formatClassList(jdkJarHellClasses));
        }
    }

    private void assertNoMissingAndViolations(Set<String> missingClasses, Set<String> violationsClasses) {
        missingClasses.removeAll(excludes);
        violationsClasses.removeAll(excludes);
        String missingText = formatClassList(missingClasses);
        String violationsText = formatClassList(violationsClasses);
        if (missingText.isEmpty() && violationsText.isEmpty()) {
            getLogger().info("Third party audit passed successfully");
        } else {
            throw new IllegalStateException(
                "Audit of third party dependencies failed:\n" +
                    (missingText.isEmpty() ?  "" : "Missing classes:\n" + missingText) +
                    (violationsText.isEmpty() ? "" : "Classes with violations:\n" + violationsText)
            );
        }
    }

    private void assertNoPointlessExclusions(Set<String> missingClasses, Set<String> violationsClasses, Set<String> jdkJarHellClasses) {
        // keep our whitelist up to date
        Set<String> bogusExclusions = new TreeSet<>(excludes);
        bogusExclusions.removeAll(missingClasses);
        bogusExclusions.removeAll(jdkJarHellClasses);
        bogusExclusions.removeAll(violationsClasses);
        if (bogusExclusions.isEmpty() == false) {
            throw new IllegalStateException(
                "Invalid exclusions, nothing is wrong with these classes: " + formatClassList(bogusExclusions)
            );
        }
    }

    private String runForbiddenAPIsCli() throws IOException {
        ByteArrayOutputStream errorOut = new ByteArrayOutputStream();
        getProject().javaexec(spec -> {
            spec.setExecutable(javaHome + "/bin/java");
            spec.classpath(
                getForbiddenAPIsConfiguration(),
                getRuntimeConfiguration(),
                getCompileOnlyConfiguration()
            );
            spec.setMain("de.thetaphi.forbiddenapis.cli.CliMain");
            spec.args(
                "-f", getSignatureFile().getAbsolutePath(),
                "-d", getJarExpandDir(),
                "--allowmissingclasses"
            );
            spec.setErrorOutput(errorOut);
            if (getLogger().isInfoEnabled() == false) {
                spec.setStandardOutput(new NullOutputStream());
            }
            spec.setIgnoreExitValue(true);
        });
        final String forbiddenApisOutput;
        try (ByteArrayOutputStream outputStream = errorOut) {
            forbiddenApisOutput = outputStream.toString(StandardCharsets.UTF_8.name());
        }
        if (getLogger().isInfoEnabled()) {
            getLogger().info(forbiddenApisOutput);
        }
        return forbiddenApisOutput;
    }

    private FileCollection getJarsToScan() {
        FileCollection jars = getRuntimeConfiguration()
            .fileCollection(dep -> dep.getGroup().startsWith("org.elasticsearch") == false);
        Configuration compileOnlyConfiguration = getCompileOnlyConfiguration();
        // don't scan provided dependencies that we already scanned, e.x. don't scan cores dependencies for every plugin
        if (compileOnlyConfiguration != null) {
            jars.minus(compileOnlyConfiguration);
        }
        if (jars.isEmpty()) {
            throw new StopExecutionException("No jars to scan");
        }
        return jars;
    }

    private String formatClassList(Set<String> classList) {
        return classList.stream()
            .map(name -> "  * " + name)
            .collect(Collectors.joining("\n"));
    }

    private Set<String> runJdkJarHellCheck() throws IOException {
        ByteArrayOutputStream standardOut = new ByteArrayOutputStream();
        ExecResult execResult = getProject().javaexec(spec -> {
            URL location = JdkJarHellCheck.class.getProtectionDomain().getCodeSource().getLocation();
            if (location.getProtocol().equals("file") == false) {
                throw new GradleException("Unexpected location for JdkJarHellCheck class: " + location);
            }
            try {
                spec.classpath(
                    location.toURI().getPath(),
                    getRuntimeConfiguration(),
                    getCompileOnlyConfiguration()
                );
            } catch (URISyntaxException e) {
                throw new AssertionError(e);
            }
            spec.setMain(JdkJarHellCheck.class.getName());
            spec.args(getJarExpandDir());
            spec.setIgnoreExitValue(true);
            spec.setExecutable(javaHome + "/bin/java");
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


}
