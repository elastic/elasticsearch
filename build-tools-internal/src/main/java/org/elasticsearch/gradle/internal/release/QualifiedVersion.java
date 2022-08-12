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
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encapsulates comparison and printing logic for an x.y.z version with optional qualifier. This class is very similar
 * to {@link Version}, but it dissects the qualifier in such a way that is incompatible
 * with how {@link Version} is used in the build. It also retains any qualifier (prerelease) information, and uses
 * that information when comparing instances.
 */
public record QualifiedVersion(int major, int minor, int revision, Qualifier qualifier) implements Comparable<QualifiedVersion> {

    private static final Pattern pattern = Pattern.compile(
        "^v? (\\d+) \\. (\\d+) \\. (\\d+) (?: - (alpha\\d+ | beta\\d+ | rc\\d+ | SNAPSHOT ) )? $",
        Pattern.COMMENTS
    );

    /**
     * Parses the supplied string into an object.
     *
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
            matcher.group(4) == null ? null : Qualifier.of(matcher.group(4))
        );
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "%d.%d.%d%s", major, minor, revision, qualifier == null ? "" : "-" + qualifier);
    }

    public boolean hasQualifier() {
        return qualifier != null;
    }

    public boolean isSnapshot() {
        return this.qualifier != null && this.qualifier.level == QualifierLevel.SNAPSHOT;
    }

    public QualifiedVersion withoutQualifier() {
        return new QualifiedVersion(major, minor, revision, null);
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

    private record Qualifier(QualifierLevel level, int number) implements Comparable<Qualifier> {

        private static final Comparator<Qualifier> COMPARATOR = Comparator.comparing((Qualifier p) -> p.level).thenComparing(p -> p.number);

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

        @Override
        public int compareTo(Qualifier other) {
            return COMPARATOR.compare(this, other);
        }

        @Override
        public String toString() {
            return level == QualifierLevel.SNAPSHOT ? level.name() : level.name() + number;
        }
    }
}
