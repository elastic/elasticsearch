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
    private final boolean snapshot;
    /**
     * Suffix on the version name.
     */
    private final String suffix;

    private static final Pattern pattern =
            Pattern.compile("(\\d)+\\.(\\d+)\\.(\\d+)(-alpha\\d+|-beta\\d+|-rc\\d+)?(-SNAPSHOT)?");

    public Version(int major, int minor, int revision, String suffix, boolean snapshot) {
        Objects.requireNonNull(major, "major version can't be null");
        Objects.requireNonNull(minor, "minor version can't be null");
        Objects.requireNonNull(revision, "revision version can't be null");
        this.major = major;
        this.minor = minor;
        this.revision = revision;
        this.snapshot = snapshot;
        this.suffix = suffix == null ? "" : suffix;

        int suffixOffset = 0;
        if (this.suffix.isEmpty()) {
            // no suffix will be considered smaller, uncomment to change that
            // suffixOffset = 100;
        } else {
            if (this.suffix.contains("alpha")) {
                suffixOffset += parseSuffixNumber(this.suffix.substring(6));
            } else if (this.suffix.contains("beta")) {
                suffixOffset += 25 + parseSuffixNumber(this.suffix.substring(5));
            } else if (this.suffix.contains("rc")) {
                suffixOffset += 50 + parseSuffixNumber(this.suffix.substring(3));
            }
            else {
                throw new IllegalArgumentException("Suffix must contain one of: alpha, beta or rc");
            }
        }

        // currently snapshot is not taken into account
        this.id = major * 10000000 + minor * 100000 + revision * 1000 + suffixOffset * 10 /*+ (snapshot ? 1 : 0)*/;
    }

    private static int parseSuffixNumber(String substring) {
        if (substring.isEmpty()) {
            throw new IllegalArgumentException("Invalid suffix, must contain a number e.x. alpha2");
        }
        return Integer.parseInt(substring);
    }

    public static Version fromString(final String s) {
        Objects.requireNonNull(s);
        Matcher matcher = pattern.matcher(s);
        if (matcher.matches() == false) {
            throw new IllegalArgumentException(
                "Invalid version format: '" + s + "'. Should be major.minor.revision[-(alpha|beta|rc)Number][-SNAPSHOT]"
            );
        }

        return new Version(
                Integer.parseInt(matcher.group(1)),
                parseSuffixNumber(matcher.group(2)),
                parseSuffixNumber(matcher.group(3)),
                matcher.group(4),
                matcher.group(5) != null
        );
    }

    @Override
    public String toString() {
        final String snapshotStr = snapshot ? "-SNAPSHOT" : "";
        return String.valueOf(getMajor()) + "." + String.valueOf(getMinor()) + "." + String.valueOf(getRevision()) +
                (suffix == null ? "" : suffix) + snapshotStr;
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

    public boolean onOrBeforeIncludingSuffix(Version otherVersion) {
        if (id != otherVersion.getId()) {
            return id < otherVersion.getId();
        }

        if (suffix.equals("")) {
            return otherVersion.getSuffix().equals("");
        }


        return otherVersion.getSuffix().equals("") || suffix.compareTo(otherVersion.getSuffix()) < 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Version version = (Version) o;
        return major == version.major &&
                minor == version.minor &&
                revision == version.revision &&
                id == version.id &&
                snapshot == version.snapshot &&
                Objects.equals(suffix, version.suffix);
    }

    @Override
    public int hashCode() {

        return Objects.hash(major, minor, revision, id, snapshot, suffix);
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

    public boolean isSnapshot() {
        return snapshot;
    }

    public String getSuffix() {
        return suffix;
    }

    @Override
    public int compareTo(Version other) {
        return Integer.compare(getId(), other.getId());
    }
}
