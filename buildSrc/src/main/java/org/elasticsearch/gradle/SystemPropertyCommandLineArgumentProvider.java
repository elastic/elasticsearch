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
package org.elasticsearch.gradle;

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
