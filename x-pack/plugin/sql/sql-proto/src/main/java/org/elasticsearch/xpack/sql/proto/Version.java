/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

public class Version {

    public final int id;
    public final String version; // originally provided String representation
    public final byte major;
    public final byte minor;
    public final byte revision;

    protected Version(String version, byte... parts) {
        this.version = version;

        assert parts.length >= 3 : "Version must be initialized with all Major.Minor.Revision components";
        this.major = parts[0];
        this.minor = parts[1];
        this.revision = parts[2];

        id = Integer.valueOf(major) * 1000000
            + Integer.valueOf(minor) * 10000
            + Integer.valueOf(revision) * 100;
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

    @Override
    public String toString() {
        return major + "." + minor + "." + revision;
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
}
