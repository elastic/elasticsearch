/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.blobstore.testkit.integrity;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Details of an index in a specific snapshot, identifying its corresponding {@link org.elasticsearch.cluster.metadata.IndexMetadata} blob
 * and the number of shards.
 */
public record IndexDescription(IndexId indexId, @Nullable String indexMetadataBlob, int shardCount) implements Writeable, ToXContentObject {

    public IndexDescription {
        if (indexId == null || shardCount < 0) {
            throw new IllegalArgumentException("invalid IndexDescription");
        }
    }

    public IndexDescription(StreamInput in) throws IOException {
        this(new IndexId(in), in.readOptionalString(), in.readVInt());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        indexId.writeTo(out);
        out.writeOptionalString(indexMetadataBlob);
        out.writeVInt(shardCount);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", indexId.getName());
        builder.field("uuid", indexId.getId());
        if (indexMetadataBlob != null) {
            builder.field("metadata_blob", indexMetadataBlob);
        }
        if (shardCount > 0) {
            builder.field("shards", shardCount);
        }
        return builder.endObject();
    }
}
