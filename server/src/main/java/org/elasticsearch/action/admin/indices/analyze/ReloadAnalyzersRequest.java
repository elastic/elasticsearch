/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.analyze;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.TransportVersion.V_8_500_033;

/**
 * Request for reloading index search analyzers
 */
public class ReloadAnalyzersRequest extends BroadcastRequest<ReloadAnalyzersRequest> {
    private final String resource;
    private final boolean preview;

    private static final TransportVersion PREVIEW_OPTION_TRANSPORT_VERSION = V_8_500_033;

    /**
     * Constructs a request for reloading index search analyzers
     * @param resource changed resource to reload analyzers from, @null if not applicable
     * @param preview {@code false} applies analyzer reloading. {@code true} previews the reloading operation, so analyzers are not reloaded
     * but the results retrieved. This is useful for understanding analyzers usage in the different indices.
     * @param indices the indices to reload analyzers for
     */
    public ReloadAnalyzersRequest(String resource, boolean preview, String... indices) {
        super(indices);
        this.resource = resource;
        this.preview = preview;
    }

    public ReloadAnalyzersRequest(StreamInput in) throws IOException {
        super(in);
        this.resource = in.readOptionalString();
        this.preview = in.getTransportVersion().onOrAfter(PREVIEW_OPTION_TRANSPORT_VERSION) && in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(resource);
        if (out.getTransportVersion().onOrAfter(PREVIEW_OPTION_TRANSPORT_VERSION)) {
            out.writeBoolean(preview);
        }
    }

    public String resource() {
        return resource;
    }

    public boolean preview() {
        return preview;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReloadAnalyzersRequest that = (ReloadAnalyzersRequest) o;
        return preview == that.preview && Objects.equals(resource, that.resource);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicesOptions(), Arrays.hashCode(indices), resource, preview);
    }

}
