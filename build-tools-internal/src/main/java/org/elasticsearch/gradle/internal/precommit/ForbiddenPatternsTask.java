/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.precommit;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;

/**
 * Checks for patterns in source files for the project which are forbidden.
 */
public abstract class ForbiddenPatternsTask extends DefaultTask {

    /*
     * A pattern set of which files should be checked.
     */
    private final PatternFilterable filesFilter = new PatternSet()
        // we always include all source files, and exclude what should not be checked
        .include("**")
        // exclude known binary extensions
        .exclude("**/*.gz")
        .exclude("**/*.ico")
        .exclude("**/*.jar")
        .exclude("**/*.zip")
        .exclude("**/*.jks")
        .exclude("**/*.crt")
        .exclude("**/*.p12")
        .exclude("**/*.keystore")
        .exclude("**/*.png")
        // vim swap file - included here to stop the build falling over if you happen to have a file open :-|
        .exclude("**/.*.swp");

    /*
     * The rules: a map from the rule name, to a rule regex pattern.
     */
    private final Map<String, String> patterns = new HashMap<>();
    private final ProjectLayout projectLayout;

    @Inject
    public ForbiddenPatternsTask(ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
        setDescription("Checks source files for invalid patterns like nocommits or tabs");
        getInputs().property("excludes", filesFilter.getExcludes());
        getInputs().property("rules", patterns);

        // add mandatory rules
        patterns.put("nocommit", "nocommit|NOCOMMIT");
        patterns.put("nocommit should be all lowercase or all uppercase", "((?i)nocommit)(?<!(nocommit|NOCOMMIT))");
        patterns.put("tab", "\t");
    }

    @InputFiles
    @IgnoreEmptyDirectories
    @PathSensitive(PathSensitivity.RELATIVE)
    @SkipWhenEmpty
    public FileCollection getFiles() {
        return getSourceFolders().get()
            .stream()
            .map(sourceFolder -> sourceFolder.matching(filesFilter))
            .reduce(FileTree::plus)
            .orElse(projectLayout.files().getAsFileTree());
    }

    @TaskAction
    public void checkInvalidPatterns() throws IOException {
        Pattern allPatterns = Pattern.compile("(" + String.join(")|(", getPatterns().values()) + ")");
        List<String> failures = new ArrayList<>();
        for (File f : getFiles()) {
            List<String> lines;
            try (Stream<String> stream = Files.lines(f.toPath(), StandardCharsets.UTF_8)) {
                lines = stream.collect(Collectors.toList());
            } catch (UncheckedIOException e) {
                throw new IllegalArgumentException("Failed to read " + f + " as UTF_8", e);
            }

            URI baseUri = getRootDir().orElse(projectLayout.getProjectDirectory().getAsFile()).get().toURI();
            String path = baseUri.relativize(f.toURI()).toString();
            IntStream.range(0, lines.size())
                .filter(i -> allPatterns.matcher(lines.get(i)).find())
                .mapToObj(l -> new AbstractMap.SimpleEntry<>(l + 1, lines.get(l)))
                .flatMap(
                    kv -> patterns.entrySet()
                        .stream()
                        .filter(p -> Pattern.compile(p.getValue()).matcher(kv.getValue()).find())
                        .map(p -> "- " + p.getKey() + " on line " + kv.getKey() + " of " + path)
                )
                .forEach(failures::add);
        }
        if (failures.isEmpty() == false) {
            throw new GradleException("Found invalid patterns:\n" + String.join("\n", failures));
        }

        File outputMarker = getOutputMarker();
        outputMarker.getParentFile().mkdirs();
        Files.writeString(outputMarker.toPath(), "done");
    }

    @OutputFile
    public File getOutputMarker() {
        return new File(projectLayout.getBuildDirectory().getAsFile().get(), "markers/" + getName());
    }

    @Input
    public Map<String, String> getPatterns() {
        return Collections.unmodifiableMap(patterns);
    }

    public void exclude(String... excludes) {
        filesFilter.exclude(excludes);
    }

    public void rule(Map<String, String> props) {
        String name = props.remove("name");
        if (name == null) {
            throw new InvalidUserDataException("Missing [name] for invalid pattern rule");
        }
        String pattern = props.remove("pattern");
        if (pattern == null) {
            throw new InvalidUserDataException("Missing [pattern] for invalid pattern rule");
        }
        if (props.isEmpty() == false) {
            throw new InvalidUserDataException("Unknown arguments for ForbiddenPatterns rule mapping: " + props.keySet().toString());
        }
        // TODO: fail if pattern contains a newline, it won't work (currently)
        patterns.put(name, pattern);
    }

    @Internal
    abstract ListProperty<FileTree> getSourceFolders();

    @Internal
    abstract Property<File> getRootDir();

    @Input
    @Optional
    Provider<String> getRootDirPath() {
        return getRootDir().map(t -> t.toPath().relativize(projectLayout.getProjectDirectory().getAsFile().toPath()).toString());
    }

}
