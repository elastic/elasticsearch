/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.Version;

import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encapsulates comparison and printing logic for an x.y.z version with optional qualifier. This class is very similar
 * to {@link Version}, but it dissects the qualifier in such a way that is incompatible
 * with how {@link Version} is used in the build.
 */
public final class QualifiedVersion implements Comparable<QualifiedVersion> {
    private final int major;
    private final int minor;
    private final int revision;
    private final Qualifier qualifier;

    private static final Pattern pattern = Pattern.compile(
        "^v? (\\d+) \\. (\\d+) \\. (\\d+) (?: - (alpha\\d+ | beta\\d+ | rc\\d+ | SNAPSHOT ) )? $",
        Pattern.COMMENTS
    );

    private QualifiedVersion(int major, int minor, int revision, String qualifier) {
        this.major = major;
        this.minor = minor;
        this.revision = revision;
        this.qualifier = qualifier == null ? null : Qualifier.of(qualifier);
    }

    /**
     * Parses the supplied string into an object.
     * @param s a version string in strict semver
     * @return a new instance
     */
    public static QualifiedVersion of(final String s) {
        Objects.requireNonNull(s);
        Matcher matcher = pattern.matcher(s);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException("Invalid version format: '" + s + "'. Should be " + pattern);
        }

        return new QualifiedVersion(
            Integer.parseInt(matcher.group(1)),
            Integer.parseInt(matcher.group(2)),
            Integer.parseInt(matcher.group(3)),
            matcher.group(4)
        );
    }

    @Override
    public String toString() {
        return "%d.%d.%d%s".formatted(major, minor, revision, qualifier == null ? "" : "-" + qualifier);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QualifiedVersion version = (QualifiedVersion) o;
        return major == version.major
            && minor == version.minor
            && revision == version.revision
            && Objects.equals(qualifier, version.qualifier);
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, revision, qualifier);
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

    public boolean hasQualifier() {
        return qualifier != null;
    }

    public Qualifier getQualifier() {
        return qualifier;
    }

    public boolean isSnapshot() {
        return this.qualifier != null && this.qualifier.level == QualifierLevel.SNAPSHOT;
    }

    private static final Comparator<QualifiedVersion> COMPARATOR = Comparator.comparing((QualifiedVersion v) -> v.major)
        .thenComparing(v -> v.minor)
        .thenComparing(v -> v.revision)
        .thenComparing((QualifiedVersion v) -> v.qualifier, Comparator.nullsLast(Comparator.naturalOrder()));

    @Override
    public int compareTo(QualifiedVersion other) {
        return COMPARATOR.compare(this, other);
    }

    public boolean isBefore(QualifiedVersion other) {
        return this.compareTo(other) < 0;
    }

    private enum QualifierLevel {
        alpha,
        beta,
        rc,
        SNAPSHOT
    }

    private static class Qualifier implements Comparable<Qualifier> {
        private final QualifierLevel level;
        private final int number;

        private Qualifier(QualifierLevel level, int number) {
            this.level = level;
            this.number = number;
        }

        private static final Comparator<Qualifier> COMPARATOR = Comparator.comparing((Qualifier p) -> p.level).thenComparing(p -> p.number);

        @Override
        public int compareTo(Qualifier other) {
            return COMPARATOR.compare(this, other);
        }

        private static Qualifier of(String qualifier) {
            if ("SNAPSHOT".equals(qualifier)) {
                return new Qualifier(QualifierLevel.SNAPSHOT, 0);
            }

            Pattern pattern = Pattern.compile("^(alpha|beta|rc)(\\d+)$");
            Matcher matcher = pattern.matcher(qualifier);
            if (matcher.find()) {
                String level = matcher.group(1);
                int number = Integer.parseInt(matcher.group(2));
                return new Qualifier(QualifierLevel.valueOf(level), number);
            } else {
                // This shouldn't happen - we check the format before this is called
                throw new IllegalArgumentException("Invalid qualifier [" + qualifier + "] passed");
            }
        }

        public String toString() {
            return this.level.name() + this.number;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Qualifier that = (Qualifier) o;
            return number == that.number && level == that.level;
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, number);
        }
    }
}
