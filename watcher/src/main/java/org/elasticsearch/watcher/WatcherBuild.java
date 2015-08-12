/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 */
public class WatcherBuild {

    public static final WatcherBuild CURRENT;

    static {
        String versionName = "NA";
        String hash = "NA";
        String hashShort = "NA";
        String timestamp = "NA";

        try (InputStream is = WatcherBuild.class.getResourceAsStream("/watcher-build.properties")) {
            Properties props = new Properties();
            props.load(is);
            hash = props.getProperty("hash", hash);
            if (!hash.equals("NA")) {
                hashShort = hash.substring(0, 7);
            }
            String gitTimestampRaw = props.getProperty("timestamp");
            if (gitTimestampRaw != null) {
                timestamp = ISODateTimeFormat.dateTimeNoMillis().withZone(DateTimeZone.UTC).print(Long.parseLong(gitTimestampRaw));
            }
            versionName = props.getProperty("version", "NA");
        } catch (Exception e) {
            // just ignore...
        }

        CURRENT = new WatcherBuild(versionName, hash, hashShort, timestamp);
    }

    private final String versionName;
    private final String hash;
    private final String hashShort;
    private final String timestamp;

    WatcherBuild(String versionName, String hash, String hashShort, String timestamp) {
        this.versionName = versionName;
        this.hash = hash;
        this.hashShort = hashShort;
        this.timestamp = timestamp;
    }

    public String versionName() {
        return versionName;
    }

    public String hash() {
        return hash;
    }

    public String hashShort() {
        return hashShort;
    }

    public String timestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WatcherBuild that = (WatcherBuild) o;

        if (!hash.equals(that.hash)) return false;
        if (!hashShort.equals(that.hashShort)) return false;
        if (!timestamp.equals(that.timestamp)) return false;
        if (!versionName.equals(that.versionName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = hash.hashCode();
        result = 31 * result + hashShort.hashCode();
        result = 31 * result + timestamp.hashCode();
        result = 31 * result + versionName.hashCode();
        return result;
    }

    public static WatcherBuild readBuild(StreamInput in) throws IOException {
        String versionName = in.readString();
        String hash = in.readString();
        String hashShort = in.readString();
        String timestamp = in.readString();
        return new WatcherBuild(versionName, hash, hashShort, timestamp);
    }

    public static void writeBuild(WatcherBuild build, StreamOutput out) throws IOException {
        out.writeString(build.versionName);
        out.writeString(build.hash);
        out.writeString(build.hashShort);
        out.writeString(build.timestamp);
    }
}
