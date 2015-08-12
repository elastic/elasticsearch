/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 */
public class ShieldBuild {

    public static final ShieldBuild CURRENT;

    static {
        String hash = "NA";
        String hashShort = "NA";
        String timestamp = "NA";

        try (InputStream is = ShieldBuild.class.getResourceAsStream("/shield-build.properties")) {
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
        } catch (Exception e) {
            // just ignore...
        }

        CURRENT = new ShieldBuild(hash, hashShort, timestamp);
    }

    private String hash;
    private String hashShort;
    private String timestamp;

    ShieldBuild(String hash, String hashShort, String timestamp) {
        this.hash = hash;
        this.hashShort = hashShort;
        this.timestamp = timestamp;
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

    public static ShieldBuild readBuild(StreamInput in) throws IOException {
        String hash = in.readString();
        String hashShort = in.readString();
        String timestamp = in.readString();
        return new ShieldBuild(hash, hashShort, timestamp);
    }

    public static void writeBuild(ShieldBuild build, StreamOutput out) throws IOException {
        out.writeString(build.hash());
        out.writeString(build.hashShort());
        out.writeString(build.timestamp());
    }
}
