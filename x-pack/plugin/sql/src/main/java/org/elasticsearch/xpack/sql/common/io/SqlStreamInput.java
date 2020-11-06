/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.common.io;

import org.elasticsearch.Version;
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

    private final ZoneId zoneId;

    public SqlStreamInput(String base64encoded, NamedWriteableRegistry namedWriteableRegistry, Version version) throws IOException {
        this(Base64.getDecoder().decode(base64encoded), namedWriteableRegistry, version);
    }

    public SqlStreamInput(byte[] input, NamedWriteableRegistry namedWriteableRegistry, Version version) throws IOException {
        super(StreamInput.wrap(input), namedWriteableRegistry);

        // version check first
        Version ver = Version.readVersion(delegate);
        if (version.compareTo(ver) != 0) {
            throw new SqlIllegalArgumentException("Unsupported cursor version [{}], expected [{}]", ver, version);
        }
        delegate.setVersion(version);
        // configuration settings
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
