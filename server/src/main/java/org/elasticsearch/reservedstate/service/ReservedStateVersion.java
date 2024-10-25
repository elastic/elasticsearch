/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.env.BuildVersion;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * File settings metadata class that holds information about
 * versioning and Elasticsearch version compatibility
 */
public record ReservedStateVersion(Long version, BuildVersion buildVersion) implements Writeable {

    public static final ParseField VERSION = new ParseField("version");
    public static final ParseField COMPATIBILITY = new ParseField("compatibility");

    private static final ConstructingObjectParser<ReservedStateVersion, Void> PARSER = new ConstructingObjectParser<>(
        "reserved_cluster_state_version_metadata",
        a -> {
            Long updateId = Long.parseLong((String) a[0]);
            BuildVersion minCompatVersion = BuildVersion.fromString((String) a[1]);

            return new ReservedStateVersion(updateId, minCompatVersion);
        }
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), VERSION);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), COMPATIBILITY);
    }

    public static ReservedStateVersion parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static ReservedStateVersion readFrom(StreamInput input) throws IOException {
        return new ReservedStateVersion(input.readLong(), BuildVersion.fromVersionId(input.readVInt()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version());
        out.writeVInt(buildVersion().id());
    }
}
