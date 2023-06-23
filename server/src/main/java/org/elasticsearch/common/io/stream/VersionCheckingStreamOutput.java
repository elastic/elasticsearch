/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Nullable;

import java.io.IOException;

/**
 * This {@link StreamOutput} writes nowhere. It can be used to check if serialization would
 * be successful writing to a specific version.
 */
public class VersionCheckingStreamOutput extends StreamOutput {

    public VersionCheckingStreamOutput(TransportVersion version) {
        setTransportVersion(version);
    }

    @Override
    public void writeByte(byte b) throws IOException {
        // no-op
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        // no-op
    }

    @Override
    public void flush() throws IOException {
        // no-op

    }

    @Override
    public void close() throws IOException {
        // no-op

    }

    @Override
    public void writeNamedWriteable(NamedWriteable namedWriteable) throws IOException {
        if (namedWriteable instanceof VersionedNamedWriteable vnw) {
            checkVersionCompatibility(vnw);
        }
        super.writeNamedWriteable(namedWriteable);
    }

    @Override
    public void writeOptionalNamedWriteable(@Nullable NamedWriteable namedWriteable) throws IOException {
        if (namedWriteable != null && namedWriteable instanceof VersionedNamedWriteable vnw) {
            checkVersionCompatibility(vnw);
        }
        super.writeOptionalNamedWriteable(namedWriteable);
    }

    private void checkVersionCompatibility(VersionedNamedWriteable namedWriteable) {
        if (namedWriteable.getMinimalSupportedVersion().after(getTransportVersion())) {
            throw new IllegalArgumentException(
                "["
                    + namedWriteable.getWriteableName()
                    + "] was released first in version "
                    + namedWriteable.getMinimalSupportedVersion()
                    + ", failed compatibility check trying to send it to node with version "
                    + getTransportVersion()
            );
        }
    }
}
