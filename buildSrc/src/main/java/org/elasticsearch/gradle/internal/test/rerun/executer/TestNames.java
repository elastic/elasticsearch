/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.test.rerun.executer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

public final class TestNames {

    private final Map<String, Set<String>> map = new HashMap<>();

    public void add(String className, String testName) {
        map.computeIfAbsent(className, ignored -> new HashSet<>()).add(testName);
    }

    public void remove(String className, Predicate<? super String> predicate) {
        Set<String> testNames = map.get(className);
        if (testNames != null) {
            testNames.removeIf(predicate);
            if (testNames.isEmpty()) {
                map.remove(className);
            }
        }
    }

    public boolean remove(String className, String testName) {
        Set<String> testNames = map.get(className);
        if (testNames == null) {
            return false;
        } else {
            if (testNames.remove(testName)) {
                if (testNames.isEmpty()) {
                    map.remove(className);
                }
                return true;
            } else {
                return false;
            }
        }
    }

    public Stream<Map.Entry<String, Set<String>>> stream() {
        return map.entrySet().stream();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public int size() {
        return stream().mapToInt(s -> s.getValue().size()).sum();
    }
}
