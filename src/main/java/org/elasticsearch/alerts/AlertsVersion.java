/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts;

import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.io.Serializable;

/**
 */
@SuppressWarnings("deprecation")
public class AlertsVersion implements Serializable {

    // The logic for ID is: XXYYZZAA, where XX is major version, YY is minor version, ZZ is revision, and AA is Beta/RC indicator
    // AA values below 50 are beta builds, and below 99 are RC builds, with 99 indicating a release
    // the (internal) format of the id is there so we can easily do after/before checks on the id

    // The first internal beta has already been released, without this class being here, so we start version version 2.
    public static final int V_1_0_0_Beta2_ID = /*00*/1000002;
    public static final AlertsVersion V_1_0_0_Beta2 = new AlertsVersion(V_1_0_0_Beta2_ID, true, Version.V_1_4_0);

    public static final AlertsVersion CURRENT = V_1_0_0_Beta2;

    public static AlertsVersion readVersion(StreamInput in) throws IOException {
        return fromId(in.readVInt());
    }

    public static AlertsVersion fromId(int id) {
        switch (id) {
            case V_1_0_0_Beta2_ID:
                return V_1_0_0_Beta2;

            default:
                return new AlertsVersion(id, null, Version.CURRENT);
        }
    }

    public static void writeVersion(AlertsVersion version, StreamOutput out) throws IOException {
        out.writeVInt(version.id);
    }

    /**
     * Returns the smallest version between the 2.
     */
    public static AlertsVersion smallest(AlertsVersion version1, AlertsVersion version2) {
        return version1.id < version2.id ? version1 : version2;
    }

    /**
     * Returns the version given its string representation, current version if the argument is null or empty
     */
    public static AlertsVersion fromString(String version) {
        if (!Strings.hasLength(version)) {
            return AlertsVersion.CURRENT;
        }

        String[] parts = version.split("\\.");
        if (parts.length < 3 || parts.length > 4) {
            throw new IllegalArgumentException("the version needs to contain major, minor and revision, and optionally the build");
        }

        try {
            //we reverse the version id calculation based on some assumption as we can't reliably reverse the modulo
            int major = Integer.parseInt(parts[0]) * 1000000;
            int minor = Integer.parseInt(parts[1]) * 10000;
            int revision = Integer.parseInt(parts[2]) * 100;

            int build = 99;
            if (parts.length == 4) {
                String buildStr = parts[3];
                if (buildStr.startsWith("Beta")) {
                    build = Integer.parseInt(buildStr.substring(4));
                }
                if (buildStr.startsWith("RC")) {
                    build = Integer.parseInt(buildStr.substring(2)) + 50;
                }
            }

            return fromId(major + minor + revision + build);

        } catch(NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse version " + version, e);
        }
    }

    public final int id;
    public final byte major;
    public final byte minor;
    public final byte revision;
    public final byte build;
    public final Boolean snapshot;
    public final Version minEsCompatibilityVersion;
    // TODO: Once licencing integration has been completed license version should be added to

    AlertsVersion(int id, @Nullable Boolean snapshot, Version minEsCompatibilityVersion) {
        this.id = id;
        this.major = (byte) ((id / 1000000) % 100);
        this.minor = (byte) ((id / 10000) % 100);
        this.revision = (byte) ((id / 100) % 100);
        this.build = (byte) (id % 100);
        this.snapshot = snapshot;
        this.minEsCompatibilityVersion = minEsCompatibilityVersion;
    }

    public boolean snapshot() {
        return snapshot != null && snapshot;
    }

    public boolean after(AlertsVersion version) {
        return version.id < id;
    }

    public boolean onOrAfter(AlertsVersion version) {
        return version.id <= id;
    }

    public boolean before(AlertsVersion version) {
        return version.id > id;
    }

    public boolean onOrBefore(AlertsVersion version) {
        return version.id >= id;
    }

    public boolean compatibleWith(AlertsVersion version) {
        return version.onOrAfter(minimumCompatibilityVersion());
    }

    public boolean compatibleWith(Version esVersion) {
        return esVersion.onOrAfter(minEsCompatibilityVersion);
    }

    /**
     * Returns the minimum compatible version based on the current
     * version. Ie a node needs to have at least the return version in order
     * to communicate with a node running the current version. The returned version
     * is in most of the cases the smallest major version release unless the current version
     * is a beta or RC release then the version itself is returned.
     */
    public AlertsVersion minimumCompatibilityVersion() {
        return AlertsVersion.smallest(this, fromId(major * 1000000 + 99));
    }

    /**
     * @return  The minimum elasticsearch version this shield version is compatible with.
     */
    public Version minimumEsCompatiblityVersion() {
        return minEsCompatibilityVersion;
    }

    /**
     * Just the version number (without -SNAPSHOT if snapshot).
     */
    public String number() {
        StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(revision);
        if (build < 50) {
            sb.append(".Beta").append(build);
        } else if (build < 99) {
            sb.append(".RC").append(build - 50);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(number());
        if (snapshot()) {
            sb.append("-SNAPSHOT");
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AlertsVersion that = (AlertsVersion) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
