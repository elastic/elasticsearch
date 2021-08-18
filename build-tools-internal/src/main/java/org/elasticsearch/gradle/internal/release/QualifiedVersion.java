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
 * with how {@link Version} is used in the build.
 */
public final class QualifiedVersion implements Comparable<QualifiedVersion> {
    private final int major;
    private final int minor;
    private final int revision;
    private final Qualifier preRelease;

    // We only support e.g. `.RC2` due to legacy tags. New tags should conform to semver.
    private static final String versionRegex =
        "^v? (\\d+) \\. (\\d+) \\. (\\d+) (?: [.-] (alpha\\d+ | beta\\d+ | rc\\d+ ) )? (?: -SNAPSHOT)? $";
    private static final Pattern pattern = Pattern.compile(versionRegex, Pattern.COMMENTS | Pattern.CASE_INSENSITIVE);

    private QualifiedVersion(int major, int minor, int revision, String preRelease) {
        this.major = major;
        this.minor = minor;
        this.revision = revision;
        this.preRelease = preRelease == null ? null : Qualifier.of(preRelease.replaceFirst("^[.-]", "").toLowerCase(Locale.ROOT));
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
            String expected = "(v)?major.minor.revision[-(alpha|beta|rc)Number][-SNAPSHOT]";
            throw new IllegalArgumentException("Invalid version format: '" + s + "'. Should be " + expected);
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
        return "%d.%d.%d%s".formatted(major, minor, revision, preRelease == null ? "" : "-" + preRelease);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QualifiedVersion version = (QualifiedVersion) o;
        return major == version.major
            && minor == version.minor
            && revision == version.revision
            && Objects.equals(preRelease, version.preRelease);
    }

    @Override
    public int hashCode() {
        return Objects.hash(major, minor, revision, preRelease);
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

    public Qualifier getPreRelease() {
        return preRelease;
    }

    public boolean isPreRelease() {
        return preRelease != null;
    }

    public boolean isFirstPreRelease() {
        return preRelease != null && preRelease.level == QualifierLevel.alpha && preRelease.number == 1;
    }

    private static final Comparator<QualifiedVersion> COMPARATOR = Comparator.comparing((QualifiedVersion v) -> v.major)
        .thenComparing(v -> v.minor)
        .thenComparing(v -> v.revision)
        .thenComparing((QualifiedVersion v) -> v.preRelease, Comparator.nullsLast(Comparator.naturalOrder()));

    @Override
    public int compareTo(QualifiedVersion other) {
        return COMPARATOR.compare(this, other);
    }

    public boolean isPriorMajor(QualifiedVersion other) {
        return this.major == other.major - 1;
    }

    public boolean isBefore(QualifiedVersion other) {
        return this.compareTo(other) < 0;
    }

    private enum QualifierLevel {
        alpha,
        beta,
        rc
    }

    private static class Qualifier implements Comparable<Qualifier> {
        private final QualifierLevel level;
        private final int number;

        private Qualifier(QualifierLevel level, int number) {
            this.level = level;
            this.number = number;
        }

        private static final Comparator<Qualifier> COMPARATOR = Comparator.comparing((Qualifier p) -> p.level)
            .thenComparing(p -> p.number);

        @Override
        public int compareTo(Qualifier other) {
            return COMPARATOR.compare(this, other);
        }

        private static Qualifier of(String prerelease) {
            Pattern pattern = Pattern.compile("^(alpha|beta|rc)(\\d+)$");
            Matcher matcher = pattern.matcher(prerelease);
            if (matcher.find()) {
                String level = matcher.group(1);
                int number = Integer.parseInt(matcher.group(2));
                return new Qualifier(QualifierLevel.valueOf(level), number);
            } else {
                // This shouldn't happen - we check the format before this is called
                throw new IllegalArgumentException("Invalid prerelease passed");
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
