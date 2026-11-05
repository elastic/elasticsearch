/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.todo;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Writes this project's to-do and fix-me comments, with git blame, to a JSON file.
 * Each file that contains a marker is blamed once; files with no marker are skipped.
 */
public abstract class TodoReportTask extends DefaultTask {

    private static final Logger logger = Logging.getLogger(TodoReportTask.class);

    /** Java sources to scan. Files under the project {@code build} directory are ignored. */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSourceFiles();

    /** Gradle project path recorded on every row, for example {@code :server}. */
    @Input
    public abstract Property<String> getModulePath();

    /**
     * Absolute project directory. Used only to derive source-set names and to skip {@code build}.
     * Stored as text so the directory tree itself is not a task input.
     */
    @Input
    public abstract Property<String> getProjectDirectory();

    /**
     * Absolute settings directory (the repository root). Used to make report paths repo-relative.
     * Stored as text so the directory tree itself is not a task input.
     */
    @Input
    public abstract Property<String> getSettingsDirectory();

    /** Markers to scan for. The default is to-do and fix-me. */
    @Input
    public abstract ListProperty<String> getMarkers();

    /** This module's JSON report. */
    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void report() throws IOException, InterruptedException {
        Path projectDirectory = Path.of(getProjectDirectory().get());
        Path settingsDirectory = Path.of(getSettingsDirectory().get());
        List<String> markers = getMarkers().get();
        Map<Path, List<TodoCommentParser.Hit>> hitsByFile = new LinkedHashMap<>();
        for (var source : getSourceFiles()) {
            Path file = source.toPath();
            if (TodoLocations.isUnderBuild(projectDirectory, file)) {
                continue;
            }
            List<TodoCommentParser.Hit> hits = TodoCommentParser.find(Files.readString(file, StandardCharsets.UTF_8), markers);
            if (hits.isEmpty() == false) {
                hitsByFile.put(file, hits);
            }
        }
        List<TodoEntry> items = blame(projectDirectory, settingsDirectory, hitsByFile);
        TodoReportWriter.writeJson(getOutputFile().get().getAsFile().toPath(), items);
        logger.info(
            "{}: {} TODO, {} FIXME",
            getModulePath().get(),
            items.stream().filter(item -> item.marker().equals("TODO")).count(),
            items.stream().filter(item -> item.marker().equals("FIXME")).count()
        );
    }

    private List<TodoEntry> blame(Path projectDirectory, Path settingsDirectory, Map<Path, List<TodoCommentParser.Hit>> hitsByFile)
        throws InterruptedException {
        if (hitsByFile.isEmpty()) {
            return List.of();
        }
        int threads = Math.max(1, Math.min(hitsByFile.size(), Runtime.getRuntime().availableProcessors()));
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            String module = getModulePath().get();
            List<Future<List<TodoEntry>>> futures = new ArrayList<>();
            for (Map.Entry<Path, List<TodoCommentParser.Hit>> entry : hitsByFile.entrySet()) {
                futures.add(pool.submit(() -> entriesFor(settingsDirectory, projectDirectory, module, entry.getKey(), entry.getValue())));
            }
            List<TodoEntry> items = new ArrayList<>();
            for (Future<List<TodoEntry>> future : futures) {
                items.addAll(future.get());
            }
            return items;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (cause instanceof IOException io) {
                throw new GradleException("Failed to blame TODO comments", io);
            }
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new GradleException("Failed to blame TODO comments", cause);
        } finally {
            pool.shutdownNow();
        }
    }

    private static List<TodoEntry> entriesFor(
        Path settingsDirectory,
        Path projectDirectory,
        String module,
        Path file,
        List<TodoCommentParser.Hit> hits
    ) throws IOException, InterruptedException {
        Map<Integer, GitBlame.Line> blamed;
        try {
            blamed = GitBlame.blame(settingsDirectory, file);
        } catch (GitBlame.UntrackedFileException e) {
            return untracked(settingsDirectory, projectDirectory, module, file, hits);
        }
        List<TodoEntry> entries = new ArrayList<>();
        for (TodoCommentParser.Hit hit : hits) {
            GitBlame.Line line = blamed.get(hit.line());
            if (line == null) {
                throw new GitBlame.BlameFailedException(file, "no blame for line " + hit.line());
            }
            entries.add(entry(settingsDirectory, projectDirectory, module, file, hit, line));
        }
        return entries;
    }

    private static List<TodoEntry> untracked(
        Path settingsDirectory,
        Path projectDirectory,
        String module,
        Path file,
        List<TodoCommentParser.Hit> hits
    ) {
        List<TodoEntry> entries = new ArrayList<>();
        for (TodoCommentParser.Hit hit : hits) {
            entries.add(
                new TodoEntry(
                    hit.marker(),
                    hit.message(),
                    hit.assignee(),
                    TodoLocations.repoRelative(settingsDirectory, file),
                    module,
                    TodoLocations.area(module),
                    TodoLocations.sourceSet(projectDirectory, file),
                    hit.line(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    "untracked"
                )
            );
        }
        return entries;
    }

    private static TodoEntry entry(
        Path settingsDirectory,
        Path projectDirectory,
        String module,
        Path file,
        TodoCommentParser.Hit hit,
        GitBlame.Line line
    ) {
        return new TodoEntry(
            hit.marker(),
            hit.message(),
            hit.assignee(),
            TodoLocations.repoRelative(settingsDirectory, file),
            module,
            TodoLocations.area(module),
            TodoLocations.sourceSet(projectDirectory, file),
            hit.line(),
            line.authorName(),
            line.authorEmail(),
            line.authoredAt(),
            line.authoredDate(),
            line.commit(),
            line.blameStatus()
        );
    }
}
