/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.dangling.import_index;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

/**
 * Represents a request to import a particular dangling index, specified
 * by its UUID. The {@link #acceptDataLoss} flag must also be
 * explicitly set to true, or later validation will fail.
 */
public class ImportDanglingIndexRequest extends AcknowledgedRequest<ImportDanglingIndexRequest> {
    private final String indexUUID;
    private final boolean acceptDataLoss;

    public ImportDanglingIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUUID = in.readString();
        this.acceptDataLoss = in.readBoolean();
    }

    public ImportDanglingIndexRequest(String indexUUID, boolean acceptDataLoss) {
        super(TRAPPY_IMPLICIT_DEFAULT_MASTER_NODE_TIMEOUT, DEFAULT_ACK_TIMEOUT);
        this.indexUUID = Objects.requireNonNull(indexUUID, "indexUUID cannot be null");
        this.acceptDataLoss = acceptDataLoss;
    }

    public String getIndexUUID() {
        return indexUUID;
    }

    public boolean isAcceptDataLoss() {
        return acceptDataLoss;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "ImportDanglingIndexRequest{indexUUID='%s', acceptDataLoss=%s}", indexUUID, acceptDataLoss);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(this.indexUUID);
        out.writeBoolean(this.acceptDataLoss);
    }
}
