/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.LazyInitializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchSummary {

    static final int MAX_TASK_DESCRIPTION_CHARS = 8 * 1024;

    private final LazyInitializable<String, RuntimeException> lazyDescription;

    public BatchSummary(TaskBatcher.BatchedTask firstTask, List<TaskBatcher.BatchedTask> allTasks) {
        lazyDescription = new LazyInitializable<>(() -> {
            final Map<String, List<TaskBatcher.BatchedTask>> processTasksBySource = new HashMap<>();
            for (final var task : allTasks) {
                processTasksBySource.computeIfAbsent(task.source, s -> new ArrayList<>()).add(task);
            }
            final StringBuilder output = new StringBuilder();
            Strings.collectionToDelimitedStringWithLimit((Iterable<String>) () -> processTasksBySource.entrySet().stream().map(entry -> {
                String tasks = firstTask.describeTasks(entry.getValue());
                return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
            }).filter(s -> s.isEmpty() == false).iterator(), ", ", "", "", MAX_TASK_DESCRIPTION_CHARS, output);
            if (output.length() > MAX_TASK_DESCRIPTION_CHARS) {
                output.append(" (").append(allTasks.size()).append(" tasks in total)");
            }
            return output.toString();
        });
    }

    // for tests
    public BatchSummary(String string) {
        lazyDescription = new LazyInitializable<>(() -> string);
    }

    @Override
    public String toString() {
        return lazyDescription.getOrCompute();
    }
}
