/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.security.InvalidParameterException;

public class Version implements Comparable<Version>{

    public final int id;
    public final String version; // originally provided String representation
    public final byte major;
    public final byte minor;
    public final byte revision;

    public static final int REVISION_MULTIPLIER = 100;
    public static final int MINOR_MULTIPLIER = REVISION_MULTIPLIER * REVISION_MULTIPLIER;
    public static final int MAJOR_MULTIPLIER = REVISION_MULTIPLIER * MINOR_MULTIPLIER;



    public static final Version V_7_7_0 = new Version(7, 7, 0);

    public Version(byte major, byte minor, byte revision) {
        this(toString(major, minor, revision), major, minor, revision);
    }

    public Version(Integer major, Integer minor, Integer revision) {
        this(major.byteValue(), minor.byteValue(), revision.byteValue());
        if (major > Byte.MAX_VALUE || minor > Byte.MAX_VALUE || revision > Byte.MAX_VALUE) {
            throw new InvalidParameterException("Invalid version initialisers [" + major + ", " + minor + ", " + revision + "]");
        }
    }

    protected Version(String version, byte... parts) {
        this.version = version;

        assert parts.length >= 3 : "Version must be initialized with all Major.Minor.Revision components";
        this.major = parts[0];
        this.minor = parts[1];
        this.revision = parts[2];

        if ((major | minor | revision) < 0 || minor >= REVISION_MULTIPLIER || revision >= REVISION_MULTIPLIER) {
            throw new InvalidParameterException("Invalid version initialisers [" + major + ", " + minor + ", " + revision + "]");
        }

        id = Integer.valueOf(major) * MAJOR_MULTIPLIER
            + Integer.valueOf(minor) * MINOR_MULTIPLIER
            + Integer.valueOf(revision) * REVISION_MULTIPLIER;
    }

    public static Version fromString(String version) {
        if (version == null || version.isEmpty()) {
            return null;
        }
        return new Version(version, from(version));
    }

    protected static byte[] from(String ver) {
        String[] parts = ver.split("[.-]");
        // Allow for optional snapshot and qualifier (Major.Minor.Revision-Qualifier-SNAPSHOT)
        if (parts.length >= 3 && parts.length <= 5) {
            try {
                return new byte[] { Byte.parseByte(parts[0]), Byte.parseByte(parts[1]), Byte.parseByte(parts[2]) };
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Invalid version format [" + ver + "]: " + nfe.getMessage());
            }
        } else {
            throw new IllegalArgumentException("Invalid version format [" + ver + "]");
        }

    }

    private static String toString(byte... parts) {
        assert parts.length >= 1 : "Version must contain at least a Major component";
        String ver = String.valueOf(parts[0]);
        for (int i = 1; i < parts.length; i ++) {
            ver += "." + parts[i];
        }
        return ver;
    }

    @Override
    public String toString() {
        return toString(major, minor, revision);
    }

    public String majorMinorToString() {
        return toString(major, minor);
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (o.getClass() == getClass()) {
            return ((Version) o).id == id;
        }
        if (o.getClass() == String.class) {
            try {
                Version v = fromString((String) o);
                return this.equals(v);
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        return false;
    }

    @Override
    public int compareTo(Version o) {
        return id - o.id;
    }

    public static int majorMinorId(Version v) {
        return v.major * MAJOR_MULTIPLIER + v.minor * MINOR_MULTIPLIER;
    }

    public int compareToMajorMinor(Version o) {
        return majorMinorId(this) - majorMinorId(o);
    }
}
