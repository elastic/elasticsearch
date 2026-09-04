/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Response returned by the source node for {@code START_RELOCATION_ACTION_NAME}. Carries source-side per-phase metrics so
 * the target node can emit them on the source's behalf. On older nodes that do not support
 * {@link #RELOCATION_SOURCE_METRICS_IN_RESPONSE} the metrics payload is omitted from the wire and
 * {@link #getRelocationSourceMetrics()} returns {@code null}.
 */
class StartRelocationResponse extends TransportResponse {
    static final TransportVersion RELOCATION_SOURCE_METRICS_IN_RESPONSE = TransportVersion.fromName(
        "relocation_source_metrics_in_response"
    );

    private final RelocationSourceMetrics relocationSourceMetrics;

    StartRelocationResponse(RelocationSourceMetrics relocationSourceMetrics) {
        this.relocationSourceMetrics = Objects.requireNonNull(relocationSourceMetrics);
    }

    StartRelocationResponse(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(RELOCATION_SOURCE_METRICS_IN_RESPONSE)) {
            this.relocationSourceMetrics = new RelocationSourceMetrics(in);
        } else {
            this.relocationSourceMetrics = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert relocationSourceMetrics != null;
        if (out.getTransportVersion().supports(RELOCATION_SOURCE_METRICS_IN_RESPONSE)) {
            relocationSourceMetrics.writeTo(out);
        }
    }

    @Nullable
    RelocationSourceMetrics getRelocationSourceMetrics() {
        return relocationSourceMetrics;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj instanceof StartRelocationResponse other) {
            return Objects.equals(relocationSourceMetrics, other.relocationSourceMetrics);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(relocationSourceMetrics);
    }
}
