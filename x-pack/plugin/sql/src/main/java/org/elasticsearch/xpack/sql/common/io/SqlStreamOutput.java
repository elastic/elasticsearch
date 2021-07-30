/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.common.io;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Base64;

public class SqlStreamOutput extends OutputStreamStreamOutput {

    private final ByteArrayOutputStream bytes;

    public SqlStreamOutput(Version version, ZoneId zoneId) throws IOException {
        this(new ByteArrayOutputStream(), version, zoneId);
    }

    private SqlStreamOutput(ByteArrayOutputStream bytes, Version version, ZoneId zoneId) throws IOException {
        super(Base64.getEncoder().wrap(new OutputStreamStreamOutput(bytes)));
        this.bytes = bytes;

        Version.writeVersion(version, this);
        writeZoneId(zoneId);
    }

    /**
     * Should be called _after_ closing the stream - there are no guarantees otherwise.
     */
    public String streamAsString() {
        // Base64 uses this encoding instead of UTF-8
        return bytes.toString(StandardCharsets.ISO_8859_1);
    }
}
