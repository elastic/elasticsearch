/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.common.io;

import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Base64;

/**
 * SQL-specific stream extension for {@link StreamInput} used for deserializing
 * SQL components, especially on the client-side.
 */
public class SqlStreamInput extends NamedWriteableAwareStreamInput {

    public static SqlStreamInput fromString(String base64encoded, NamedWriteableRegistry namedWriteableRegistry, Version version)
        throws IOException {
        byte[] bytes = Base64.getDecoder().decode(base64encoded);
        StreamInput in = StreamInput.wrap(bytes);
        Version inVersion = Version.readVersion(in);
        if (version.compareTo(inVersion) != 0) {
            throw new SqlIllegalArgumentException("Unsupported cursor version [{}], expected [{}]", inVersion, version);
        }

        InputStreamStreamInput uncompressingIn = new InputStreamStreamInput(CompressorFactory.COMPRESSOR.threadLocalInputStream(in));
        return new SqlStreamInput(uncompressingIn, namedWriteableRegistry, inVersion);
    }

    private final ZoneId zoneId;

    private SqlStreamInput(StreamInput input, NamedWriteableRegistry namedWriteableRegistry, Version version) throws IOException {
        super(input, namedWriteableRegistry);

        delegate.setVersion(version);
        zoneId = delegate.readZoneId();
    }

    public ZoneId zoneId() {
        return zoneId;
    }

    public static SqlStreamInput asSqlStream(StreamInput in) {
        if (in instanceof SqlStreamInput) {
            return (SqlStreamInput) in;
        }
        throw new SqlIllegalArgumentException("Expected SQL cursor stream, received [{}]", in.getClass());
    }
}
