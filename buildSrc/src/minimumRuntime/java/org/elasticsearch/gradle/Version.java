package org.elasticsearch.gradle;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Encapsulates comparison and printing logic for an x.y.z version.
 */
public final class Version implements Comparable<Version> {
    private final int major;
    private final int minor;
    private final int revision;
    private final int id;

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
        RELAXED
    }

    private static final Pattern pattern = Pattern.compile("(\\d)+\\.(\\d+)\\.(\\d+)(-alpha\\d+|-beta\\d+|-rc\\d+)?(-SNAPSHOT)?");

    private static final Pattern relaxedPattern = Pattern.compile("(\\d)+\\.(\\d+)\\.(\\d+)(-[a-zA-Z0-9_]+)*?");

    public Version(int major, int minor, int revision) {
        Objects.requireNonNull(major, "major version can't be null");
        Objects.requireNonNull(minor, "minor version can't be null");
        Objects.requireNonNull(revision, "revision version can't be null");
        this.major = major;
        this.minor = minor;
        this.revision = revision;

        // currently snapshot is not taken into account
        this.id = major * 10000000 + minor * 100000 + revision * 1000;
    }

    private static int parseSuffixNumber(String substring) {
        if (substring.isEmpty()) {
            throw new IllegalArgumentException("Invalid suffix, must contain a number e.x. alpha2");
        }
        return Integer.parseInt(substring);
    }

    public static Version fromString(final String s) {
        return fromString(s, Mode.STRICT);
    }

    public static Version fromString(final String s, final Mode mode) {
        Objects.requireNonNull(s);
        Matcher matcher = mode == Mode.STRICT ? pattern.matcher(s) : relaxedPattern.matcher(s);
        if (matcher.matches() == false) {
            String expected = mode == Mode.STRICT == true
                ? "major.minor.revision[-(alpha|beta|rc)Number][-SNAPSHOT]"
                : "major.minor.revision[-extra]";
            throw new IllegalArgumentException("Invalid version format: '" + s + "'. Should be " + expected);
        }

        return new Version(Integer.parseInt(matcher.group(1)), parseSuffixNumber(matcher.group(2)), parseSuffixNumber(matcher.group(3)));
    }

    @Override
    public String toString() {
        return String.valueOf(getMajor()) + "." + String.valueOf(getMinor()) + "." + String.valueOf(getRevision());
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

    @Override
    public int compareTo(Version other) {
        return Integer.compare(getId(), other.getId());
    }

}
