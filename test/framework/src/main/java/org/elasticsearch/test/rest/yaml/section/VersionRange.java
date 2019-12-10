package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;

public class VersionRange {
    private final Version lower;
    private final Version upper;

    public VersionRange(Version lower, Version upper) {
        this.lower = lower;
        this.upper = upper;
    }

    public Version lower() {
        return lower;
    }

    public Version upper() {
        return upper;
    }

    public boolean contain(Version currentVersion) {
        return lower != null && upper != null && currentVersion.onOrAfter(lower)
            && currentVersion.onOrBefore(upper);
    }

    @Override
    public String toString() {
        return "[" + lower + " - " + upper + "]";
    }
}
