/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.elasticsearch.gradle.internal.release.GitWrapper;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.UntrackedTask;
import org.gradle.process.ExecOperations;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

/**
 * Resolves a git merge conflict in {@code muted-tests.yml} without a human re-reading both sides.
 * <p>
 * Conflicts in this file are mechanical: both sides appended mutes after the same last entry, or one side removed an entry next
 * to lines the other side touched. This task reads the three index stages git leaves behind (merge base, receiving branch,
 * incoming change), works out which entries the incoming change added or removed relative to the base, re-applies those to the
 * receiving branch's copy, writes the result and stages it. Entries are identified by {@code class} + {@code method}. Anything
 * that is not a plain addition or removal (an entry modified by the incoming change, or added on both sides with different
 * content) fails the task so that a human decides.
 * <p>
 * The file is edited at the text level, one entry block at a time, so comments and the formatting of untouched entries are
 * preserved.
 */
@UntrackedTask(because = "Resolves a git merge conflict in place")
public abstract class MergeMutedTestsTask extends DefaultTask {

    static final String MUTED_TESTS_FILE = "muted-tests.yml";
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final Path rootDir;
    private final GitWrapper git;

    @Inject
    public MergeMutedTestsTask(ProjectLayout projectLayout, ExecOperations execOperations) {
        this.rootDir = projectLayout.getSettingsDirectory().getAsFile().toPath();
        this.git = new GitWrapper(execOperations);
        setDescription("Resolves git merge conflicts in " + MUTED_TESTS_FILE + " by re-applying the incoming additions and removals");
    }

    @TaskAction
    public void mergeMutedTests() throws IOException {
        if (git("ls-files", "-u", "--", MUTED_TESTS_FILE).isBlank()) {
            getLogger().lifecycle("No merge conflict in {}", MUTED_TESTS_FILE);
            return;
        }
        Result result = merge(
            git("show", ":1:" + MUTED_TESTS_FILE),
            git("show", ":2:" + MUTED_TESTS_FILE),
            git("show", ":3:" + MUTED_TESTS_FILE)
        );
        Files.writeString(rootDir.resolve(MUTED_TESTS_FILE), result.content());
        git("add", MUTED_TESTS_FILE);
        getLogger().lifecycle(
            "Resolved merge conflict in {}: applied {} addition(s) and {} removal(s) from the incoming change, result staged",
            MUTED_TESTS_FILE,
            result.added(),
            result.removed()
        );
    }

    private String git(String... args) {
        List<String> command = new ArrayList<>(List.of("git", "-C", rootDir.toString()));
        command.addAll(List.of(args));
        return git.runCommand(command.toArray(String[]::new));
    }

    /** The merged file content plus how many incoming additions and removals were actually applied. */
    record Result(String content, int added, int removed) {}

    /**
     * Applies the entries added or removed between {@code base} and {@code incoming} to {@code receiving}.
     *
     * @throws GradleException when an entry was modified rather than added or removed, or when both sides added the same entry
     *                         with different content
     */
    static Result merge(String base, String receiving, String incoming) {
        Map<Key, Block> baseEntries = entries(split(base));
        Map<Key, Block> incomingEntries = entries(split(incoming));
        List<Block> result = new ArrayList<>(split(receiving));
        int added = 0;
        int removed = 0;
        for (Map.Entry<Key, Block> entry : baseEntries.entrySet()) {
            Key key = entry.getKey();
            Block incomingBlock = incomingEntries.get(key);
            if (incomingBlock == null) {
                if (result.removeIf(block -> key.equals(block.key()))) {
                    removed++;
                }
            } else if (incomingBlock.node().equals(entry.getValue().node()) == false) {
                throw new GradleException(
                    "Muted test entry ["
                        + key
                        + "] was modified rather than added or removed by the incoming change, resolve the conflict manually:\n"
                        + entry.getValue().text()
                        + "\n---\n"
                        + incomingBlock.text()
                );
            }
        }
        for (Map.Entry<Key, Block> entry : incomingEntries.entrySet()) {
            Key key = entry.getKey();
            if (baseEntries.containsKey(key)) {
                continue;
            }
            List<Block> existing = result.stream().filter(block -> key.equals(block.key())).toList();
            if (existing.isEmpty()) {
                result.add(entry.getValue());
                added++;
                continue;
            }
            for (Block block : existing) {
                if (block.node().equals(entry.getValue().node()) == false) {
                    throw new GradleException(
                        "Muted test entry ["
                            + key
                            + "] exists on both sides with different content, resolve the conflict manually:\n"
                            + block.text()
                            + "\n---\n"
                            + entry.getValue().text()
                    );
                }
            }
        }
        StringBuilder content = new StringBuilder();
        for (Block block : result) {
            for (String line : block.lines()) {
                content.append(line).append('\n');
            }
        }
        return new Result(content.toString(), added, removed);
    }

    /**
     * Splits the file into blocks: a list entry starts at a column-0 {@code - } line and continues through indented lines; any
     * other line (the {@code tests:} key, comments, blank lines) is a verbatim block of its own so it survives entry removal.
     */
    private static List<Block> split(String text) {
        List<List<String>> chunks = new ArrayList<>();
        List<String> entry = null;
        for (String line : text.lines().toList()) {
            if (line.startsWith("- ")) {
                entry = new ArrayList<>(List.of(line));
                chunks.add(entry);
            } else if (entry != null && (line.startsWith(" ") || line.startsWith("\t"))) {
                entry.add(line);
            } else {
                entry = null;
                chunks.add(List.of(line));
            }
        }
        List<Block> blocks = new ArrayList<>();
        for (List<String> chunk : chunks) {
            blocks.add(new Block(chunk, chunk.get(0).startsWith("- ") ? parseEntry(chunk) : null));
        }
        return blocks;
    }

    private static JsonNode parseEntry(List<String> lines) {
        String text = String.join("\n", lines);
        JsonNode node;
        try {
            node = YAML.readTree(text).get(0);
        } catch (IOException e) {
            throw new GradleException("Cannot parse muted test entry:\n" + text, e);
        }
        if (node == null || node.get("class") == null) {
            throw new GradleException("Muted test entry has no class:\n" + text);
        }
        return node;
    }

    private static Map<Key, Block> entries(List<Block> blocks) {
        Map<Key, Block> entries = new LinkedHashMap<>();
        for (Block block : blocks) {
            if (block.node() != null) {
                entries.put(block.key(), block);
            }
        }
        return entries;
    }

    /** One list entry (with its parsed form in {@code node}) or, when {@code node} is null, a single verbatim line. */
    private record Block(List<String> lines, JsonNode node) {
        Key key() {
            return node == null ? null : new Key(node.get("class"), node.has("method") ? node.get("method") : node.get("methods"));
        }

        String text() {
            return String.join("\n", lines);
        }
    }

    /** Identity of a muted test entry: the class plus the method (or legacy {@code methods} list), ignoring issue and tasks. */
    private record Key(JsonNode className, JsonNode method) {
        @Override
        public String toString() {
            if (method == null) {
                return className.asText();
            }
            return className.asText() + " " + (method.isTextual() ? method.asText() : method.toString());
        }
    }
}
