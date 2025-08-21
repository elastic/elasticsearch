/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.test;

import org.gradle.api.tasks.Input;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class SystemPropertyCommandLineArgumentProvider implements CommandLineArgumentProvider {
    private final Map<String, Object> systemProperties = new LinkedHashMap<>();

    public void systemProperty(String key, Supplier<String> value) {
        systemProperties.put(key, value);
    }

    public void systemProperty(String key, Object value) {
        systemProperties.put(key, value);
    }

    @Override
    public Iterable<String> asArguments() {
        return systemProperties.entrySet()
            .stream()
            .map(
                entry -> "-D"
                    + entry.getKey()
                    + "="
                    + (entry.getValue() instanceof Supplier ? ((Supplier) entry.getValue()).get() : entry.getValue())
            )
            .collect(Collectors.toList());
    }

    // Track system property keys as an input so our build cache key will change if we add properties but values are still ignored
    @Input
    public Iterable<String> getPropertyNames() {
        return systemProperties.keySet();
    }
}
