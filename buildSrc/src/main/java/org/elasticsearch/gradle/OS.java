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

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Supplier;

public enum OS {
    WINDOWS,
    MAC,
    LINUX;

    public static OS current() {
        String os = System.getProperty("os.name", "");
        if (os.startsWith("Windows")) {
            return OS.WINDOWS;
        }
        if (os.startsWith("Linux") || os.startsWith("LINUX")) {
            return OS.LINUX;
        }
        if (os.startsWith("Mac")) {
            return OS.MAC;
        }
        throw new IllegalStateException("Can't determine OS from: " + os);
    }

    public static class Conditional<T> {

        private final Map<OS, Supplier<T>> conditions = new HashMap<>();

        public Conditional<T> onWindows(Supplier<T> supplier) {
            conditions.put(WINDOWS, supplier);
            return this;
        }

        public Conditional<T> onLinux(Supplier<T> supplier) {
            conditions.put(LINUX, supplier);
            return this;
        }

        public Conditional<T> onMac(Supplier<T> supplier) {
            conditions.put(MAC, supplier);
            return this;
        }

        public Conditional<T> onUnix(Supplier<T> supplier) {
            conditions.put(MAC, supplier);
            conditions.put(LINUX, supplier);
            return this;
        }

        public T supply() {
            HashSet<OS> missingOS = new HashSet<>(Arrays.asList(OS.values()));
            missingOS.removeAll(conditions.keySet());
            if (missingOS.isEmpty() == false) {
                throw new IllegalArgumentException("No condition specified for " + missingOS);
            }
            return conditions.get(OS.current()).get();
        }

    }

    public static <T> Conditional<T> conditional() {
        return new Conditional<>();
    }

    public static Conditional<String> conditionalString() {
        return conditional();
    }

}
