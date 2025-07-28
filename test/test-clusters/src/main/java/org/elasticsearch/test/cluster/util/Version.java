/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.test.cluster.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encapsulates comparison and printing logic for an x.y.z version.
 */
public class Version implements Comparable<Version>, Serializable {
    public static final Version CURRENT;
    private static final Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)(?:-(alpha\\d+|beta\\d+|rc\\d+|SNAPSHOT))?");
    private static final Pattern relaxedPattern = Pattern.compile(
        "v?(\\d+)\\.(\\d+)(?:\\.(\\d+))?(?:[\\-+]+([a-zA-Z0-9_]+(?:-[a-zA-Z0-9]+)*))?"
    );

    private final int major;
    private final int minor;
    private final int revision;
    private final int id;
    private final String qualifier;

    static {
        Properties versionProperties = new Properties();
        try (InputStream in = Version.class.getClassLoader().getResourceAsStream("version.properties")) {
            versionProperties.load(in);
            CURRENT = Version.fromString(versionProperties.getProperty("elasticsearch"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Specifies how a version string should be parsed.
     */
    public enum Mode {
        /**
         * Strict parsing only allows known suffixes after the patch number: "-alpha", "-beta" or "-rc". The
         * suffix "-SNAPSHOT" is also allowed, either after the patch number, or after the other suffices.
         */
        STRICT,

        /**
         * Relaxed parsing allows any alphanumeric suffix after the patch number.
         */
        RELAXED;
    }

    public Version(int major, int minor, int revision) {
        this(major, minor, revision, null);
    }

    public Version(int major, int minor, int revision, String qualifier) {
        this.major = major;
        this.minor = minor;
        this.revision = revision;

        // currently qualifier is not taken into account
        this.id = major * 10000000 + minor * 100000 + revision * 1000;

        this.qualifier = qualifier;
    }

    public static Version fromString(final String s) {
        return fromString(s, Mode.STRICT);
    }

    public static Version fromString(final String s, final Mode mode) {
        Objects.requireNonNull(s);
        Matcher matcher = mode == Mode.STRICT ? pattern.matcher(s) : relaxedPattern.matcher(s);
        if (matcher.matches() == false) {
            String expected = mode == Mode.STRICT
                ? "major.minor.revision[-(alpha|beta|rc)Number|-SNAPSHOT]"
                : "major.minor.revision[-extra]";
            throw new IllegalArgumentException("Invalid version format: '" + s + "'. Should be " + expected);
        }

        String major = matcher.group(1);
        String minor = matcher.group(2);
        String revision = matcher.group(3);
        String qualifier = matcher.group(4);

        return new Version(Integer.parseInt(major), Integer.parseInt(minor), revision == null ? 0 : Integer.parseInt(revision), qualifier);
    }

    public static Optional<Version> tryParse(final String s) {
        Objects.requireNonNull(s);
        Matcher matcher = pattern.matcher(s);
        if (matcher.matches() == false) {
            return Optional.empty();
        }

        String major = matcher.group(1);
        String minor = matcher.group(2);
        String revision = matcher.group(3);
        String qualifier = matcher.group(4);

        return Optional.of(
            new Version(Integer.parseInt(major), Integer.parseInt(minor), revision == null ? 0 : Integer.parseInt(revision), qualifier)
        );
    }

    @Override
    public String toString() {
        return getMajor() + "." + getMinor() + "." + getRevision();
    }

    public boolean before(Version compareTo) {
        return id < compareTo.getId();
    }

    public boolean before(String compareTo) {
        return before(fromString(compareTo));
    }

    public boolean onOrBefore(Version compareTo) {
        return id <= compareTo.getId();
    }

    public boolean onOrBefore(String compareTo) {
        return onOrBefore(fromString(compareTo));
    }

    public boolean onOrAfter(Version compareTo) {
        return id >= compareTo.getId();
    }

    public boolean onOrAfter(String compareTo) {
        return onOrAfter(fromString(compareTo));
    }

    public boolean after(Version compareTo) {
        return id > compareTo.getId();
    }

    public boolean after(String compareTo) {
        return after(fromString(compareTo));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Version version = (Version) o;
        return major == version.major && minor == version.minor && revision == version.revision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, revision, id);
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getRevision() {
        return revision;
    }

    protected int getId() {
        return id;
    }

    public String getQualifier() {
        return qualifier;
    }

    @Override
    public int compareTo(Version other) {
        return Integer.compare(getId(), other.getId());
    }

}
