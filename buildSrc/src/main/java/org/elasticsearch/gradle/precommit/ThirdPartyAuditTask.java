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
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.StopExecutionException;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.JavaExecSpec;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ThirdPartyAuditTask extends DefaultTask {

    private static final Pattern MISSING_CLASS_PATTERN = Pattern.compile(
        "WARNING: The referenced class '(.*)' cannot be loaded\\. Please fix the classpath!"
    );

    private static final Pattern VIOLATION_PATTERN = Pattern.compile(
        "\\s\\sin ([a-zA-Z0-9$.]+) \\(.*\\)"
    );

    /**
     * patterns for classes to exclude, because we understand their issues
     */
    private Set<String> excludes = new TreeSet<>();

    private File signatureFile;

    private Action<JavaExecSpec> execAction;

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

    public Action<JavaExecSpec> getExecAction() {
        return execAction;
    }

    public void setExecAction(Action<JavaExecSpec> execAction) {
        this.execAction = execAction;
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

        // Stage all the jars
        File jarExpandDir = getJarExpandDir();
        jars.forEach(jar ->
            getProject().copy(spec -> {
                spec.from(getProject().zipTree(jar));
                spec.into(jarExpandDir);
            })
        );

        ByteArrayOutputStream errorOut = new ByteArrayOutputStream();
        getProject().javaexec(spec -> {
            execAction.execute(spec);
            spec.classpath(
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
            spec.setStandardOutput(new NullOutputStream());
            spec.setIgnoreExitValue(true);
        });
        final Set<String> missingClasses = new TreeSet<>();
        final Set<String> violationsClasses = new TreeSet<>();
        final String forbiddenApisOutput;
        try (ByteArrayOutputStream outputStream = errorOut) {
            forbiddenApisOutput = outputStream.toString(StandardCharsets.UTF_8.name());
        }
        Matcher missingMatcher = MISSING_CLASS_PATTERN.matcher(forbiddenApisOutput);
        while (missingMatcher.find()) {
            missingClasses.add(missingMatcher.group(1));
        }
        Matcher violationMatcher = VIOLATION_PATTERN.matcher(forbiddenApisOutput);
        while (violationMatcher.find()) {
            violationsClasses.add(violationMatcher.group(1));
        }

        Set<String> sheistyClasses = getSheistyClasses();

        // keep our whitelist up to date
        Set<String> bogusExclusions = new TreeSet<>(excludes);
        bogusExclusions.removeAll(missingClasses);
        bogusExclusions.removeAll(sheistyClasses);
        bogusExclusions.removeAll(violationsClasses);
        if (bogusExclusions.isEmpty() == false) {
            throw new IllegalStateException(
                "Invalid exclusions, nothing is wrong with these classes: " + formatClassList(bogusExclusions)
            );
        }

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

        sheistyClasses.removeAll(excludes);
        if (sheistyClasses.isEmpty() == false) {
            throw new IllegalStateException("Jar Hell with the JDK:" + formatClassList(sheistyClasses));
        }
    }

    private String formatClassList(Set<String> classList) {
        return classList.stream()
            .map(name -> "  * " + name)
            .collect(Collectors.joining("\n"));
    }

    private Set<String> getSheistyClasses() throws IOException {
        // system.parent = extensions loader.
        // note: for jigsaw, this evilness will need modifications (e.g. use jrt filesystem!)
        ClassLoader ext = ClassLoader.getSystemClassLoader().getParent();
        assert ext != null;

        Set<String> sheistySet = new TreeSet<>();
        Path root = getJarExpandDir().toPath();
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                String entry = root.relativize(file).toString().replace('\\', '/');
                if (entry.endsWith(".class")) {
                    if (ext.getResource(entry) != null) {
                        sheistySet.add(
                            entry
                                .replace("/", ".")
                                .replace(".class","")
                        );
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return sheistySet;
    }


}
