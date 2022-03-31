/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public final class ShardStateMetadata {

    private static final String SHARD_STATE_FILE_PREFIX = "state-";
    private static final String PRIMARY_KEY = "primary";
    private static final String INDEX_UUID_KEY = "index_uuid";
    private static final String ALLOCATION_ID_KEY = "allocation_id";

    public final String indexUUID;
    public final boolean primary;
    @Nullable
    public final AllocationId allocationId; // can be null if we read from legacy format (see fromXContent and MultiDataPathUpgrader)

    public ShardStateMetadata(boolean primary, String indexUUID, AllocationId allocationId) {
        assert indexUUID != null;
        this.primary = primary;
        this.indexUUID = indexUUID;
        this.allocationId = allocationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ShardStateMetadata that = (ShardStateMetadata) o;

        if (primary != that.primary) {
            return false;
        }
        if (indexUUID.equals(that.indexUUID) == false) {
            return false;
        }
        if (Objects.equals(allocationId, that.allocationId) == false) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = indexUUID.hashCode();
        result = 31 * result + (allocationId != null ? allocationId.hashCode() : 0);
        result = 31 * result + (primary ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "primary [" + primary + "], allocation [" + allocationId + "]";
    }

    public static final MetadataStateFormat<ShardStateMetadata> FORMAT = new MetadataStateFormat<ShardStateMetadata>(
        SHARD_STATE_FILE_PREFIX
    ) {

        @Override
        protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
            XContentBuilder xContentBuilder = super.newXContentBuilder(type, stream);
            xContentBuilder.prettyPrint();
            return xContentBuilder;
        }

        @Override
        public void toXContent(XContentBuilder builder, ShardStateMetadata shardStateMetadata) throws IOException {
            builder.field(PRIMARY_KEY, shardStateMetadata.primary);
            builder.field(INDEX_UUID_KEY, shardStateMetadata.indexUUID);
            if (shardStateMetadata.allocationId != null) {
                builder.field(ALLOCATION_ID_KEY, shardStateMetadata.allocationId);
            }
        }

        @Override
        public ShardStateMetadata fromXContent(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                return null;
            }
            Boolean primary = null;
            String currentFieldName = null;
            String indexUUID = IndexMetadata.INDEX_UUID_NA_VALUE;
            AllocationId allocationId = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (PRIMARY_KEY.equals(currentFieldName)) {
                        primary = parser.booleanValue();
                    } else if (INDEX_UUID_KEY.equals(currentFieldName)) {
                        indexUUID = parser.text();
                    } else {
                        throw new CorruptStateException("unexpected field in shard state [" + currentFieldName + "]");
                    }
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (ALLOCATION_ID_KEY.equals(currentFieldName)) {
                        allocationId = AllocationId.fromXContent(parser);
                    } else {
                        throw new CorruptStateException("unexpected object in shard state [" + currentFieldName + "]");
                    }
                } else {
                    throw new CorruptStateException("unexpected token in shard state [" + token.name() + "]");
                }
            }
            if (primary == null) {
                throw new CorruptStateException("missing value for [primary] in shard state");
            }
            return new ShardStateMetadata(primary, indexUUID, allocationId);
        }
    };
}
