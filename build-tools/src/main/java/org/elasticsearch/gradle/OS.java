/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
