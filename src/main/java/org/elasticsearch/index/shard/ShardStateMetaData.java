/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.gateway.MetaDataStateFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.regex.Pattern;

/**
 */
public final class ShardStateMetaData {

    private static final String SHARD_STATE_FILE_PREFIX = "state-";
    private static final Pattern SHARD_STATE_FILE_PATTERN = Pattern.compile(SHARD_STATE_FILE_PREFIX + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    private static final String PRIMARY_KEY = "primary";
    private static final String VERSION_KEY = "version";
    private static final String INDEX_UUID_KEY = "index_uuid" ;

    public final long version;
    public final String indexUUID;
    public final boolean primary;

    public ShardStateMetaData(long version, boolean primary, String indexUUID) {
        assert indexUUID != null;
        this.version = version;
        this.primary = primary;
        this.indexUUID = indexUUID;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ShardStateMetaData that = (ShardStateMetaData) o;

        if (primary != that.primary) {
            return false;
        }
        if (version != that.version) {
            return false;
        }
        if (indexUUID != null ? !indexUUID.equals(that.indexUUID) : that.indexUUID != null) {
          return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (version ^ (version >>> 32));
        result = 31 * result + (indexUUID != null ? indexUUID.hashCode() : 0);
        result = 31 * result + (primary ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "version [" + version + "], primary [" + primary + "]";
    }

    private static MetaDataStateFormat<ShardStateMetaData> newShardStateInfoFormat(boolean deleteOldFiles) {
        return new MetaDataStateFormat<ShardStateMetaData>(XContentType.JSON, deleteOldFiles) {

            @Override
            protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
                XContentBuilder xContentBuilder = super.newXContentBuilder(type, stream);
                xContentBuilder.prettyPrint();
                return xContentBuilder;
            }

            @Override
            public void toXContent(XContentBuilder builder, ShardStateMetaData shardStateMetaData) throws IOException {
                builder.field(VERSION_KEY, shardStateMetaData.version);
                builder.field(PRIMARY_KEY, shardStateMetaData.primary);
                builder.field(INDEX_UUID_KEY, shardStateMetaData.indexUUID);
            }

            @Override
            public ShardStateMetaData fromXContent(XContentParser parser) throws IOException {
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    return null;
                }
                long version = -1;
                Boolean primary = null;
                String currentFieldName = null;
                String indexUUID = IndexMetaData.INDEX_UUID_NA_VALUE;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (VERSION_KEY.equals(currentFieldName)) {
                            version = parser.longValue();
                        } else if (PRIMARY_KEY.equals(currentFieldName)) {
                            primary = parser.booleanValue();
                        } else if (INDEX_UUID_KEY.equals(currentFieldName)) {
                            indexUUID = parser.text();
                        } else {
                            throw new CorruptStateException("unexpected field in shard state [" + currentFieldName + "]");
                        }
                    } else {
                        throw new CorruptStateException("unexpected token in shard state [" + token.name() + "]");
                    }
                }
                if (primary == null) {
                    throw new CorruptStateException("missing value for [primary] in shard state");
                }
                if (version == -1) {
                    throw new CorruptStateException("missing value for [version] in shard state");
                }
                return new ShardStateMetaData(version, primary, indexUUID);
            }
        };
    }

    public static ShardStateMetaData load(ESLogger logger, ShardId shardId, Path... shardPaths) throws IOException {
        return MetaDataStateFormat.loadLatestState(logger, newShardStateInfoFormat(false), SHARD_STATE_FILE_PATTERN,
                shardId.toString(), shardPaths);
    }

    public static void write(ESLogger logger, String reason, ShardId shardId, ShardStateMetaData shardStateMetaData,
                                 boolean deletePreviousState, Path... shardPaths) throws IOException {
        logger.trace("{} writing shard state, reason [{}]", shardId, reason);
        MetaDataStateFormat<ShardStateMetaData> stateFormat = newShardStateInfoFormat(deletePreviousState);
        stateFormat.write(shardStateMetaData, SHARD_STATE_FILE_PREFIX, shardStateMetaData.version, shardPaths);
    }
}
