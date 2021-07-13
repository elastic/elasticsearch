/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.jdk;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class JavaVersion implements Comparable<JavaVersion> {

    private final List<Integer> version;
    private final String prePart;

    public List<Integer> getVersion() {
        return version;
    }

    private JavaVersion(List<Integer> version, String prePart) {
        this.prePart = prePart;
        if (version.size() >= 2 && version.get(0) == 1 && version.get(1) == 8) {
            // for Java 8 there is ambiguity since both 1.8 and 8 are supported,
            // so we rewrite the former to the latter
            version = new ArrayList<>(version.subList(1, version.size()));
        }
        this.version = Collections.unmodifiableList(version);
    }

    /**
     * Parses the Java version as it can be retrieved as the value of java.version or
     * java.specification.version according to JEP 223.
     *
     * @param value The version String
     */
    public static JavaVersion parse(String value) {
        Objects.requireNonNull(value);
        String prePart = null;
        if (isValid(value) == false) {
            throw new IllegalArgumentException("Java version string [" + value + "] could not be parsed.");
        }
        List<Integer> version = new ArrayList<>();
        String[] parts = value.split("-");
        String[] numericComponents;
        if (parts.length == 1) {
            numericComponents = value.split("\\.");
        } else if (parts.length == 2) {
            numericComponents = parts[0].split("\\.");
            prePart = parts[1];
        } else {
            throw new IllegalArgumentException("Java version string [" + value + "] could not be parsed.");
        }

        for (String component : numericComponents) {
            version.add(Integer.valueOf(component));
        }
        return new JavaVersion(version, prePart);
    }

    public static boolean isValid(String value) {
        return value.matches("^0*[0-9]+(\\.[0-9]+)*(-[a-zA-Z0-9]+)?$");
    }

    private static final JavaVersion CURRENT = parse(System.getProperty("java.specification.version"));

    public static JavaVersion current() {
        return CURRENT;
    }

    @Override
    public int compareTo(JavaVersion o) {
        int len = Math.max(version.size(), o.version.size());
        for (int i = 0; i < len; i++) {
            int d = (i < version.size() ? version.get(i) : 0);
            int s = (i < o.version.size() ? o.version.get(i) : 0);
            if (s < d)
                return 1;
            if (s > d)
                return -1;
        }
        if (prePart != null && o.prePart == null) {
            return -1;
        } else if (prePart == null && o.prePart != null) {
            return 1;
        } else if (prePart != null && o.prePart != null) {
            return comparePrePart(prePart, o.prePart);
        }
        return 0;
    }

    private int comparePrePart(String prePart, String otherPrePart) {
        if (prePart.matches("\\d+")) {
            return otherPrePart.matches("\\d+") ?
                (new BigInteger(prePart)).compareTo(new BigInteger(otherPrePart)) : -1;
        } else {
            return otherPrePart.matches("\\d+") ?
                1 : prePart.compareTo(otherPrePart);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || o.getClass() != getClass()) {
            return false;
        }
        return compareTo((JavaVersion) o) == 0;
    }

    @Override
    public int hashCode() {
        return version.hashCode();
    }

    @Override
    public String toString() {
        final String versionString = version.stream().map(v -> Integer.toString(v)).collect(Collectors.joining("."));
        return prePart != null ? versionString + "-" + prePart : versionString;
    }
}
