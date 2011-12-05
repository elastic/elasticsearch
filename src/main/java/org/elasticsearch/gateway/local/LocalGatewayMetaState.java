/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.gateway.local;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class LocalGatewayMetaState {

    private final long version;

    private final MetaData metaData;

    public LocalGatewayMetaState(long version, MetaData metaData) {
        this.version = version;
        this.metaData = metaData;
    }

    public long version() {
        return version;
    }

    public MetaData metaData() {
        return metaData;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long version;

        private MetaData metaData;

        public Builder state(LocalGatewayMetaState state) {
            this.version = state.version();
            this.metaData = state.metaData();
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder metaData(MetaData metaData) {
            this.metaData = metaData;
            return this;
        }

        public LocalGatewayMetaState build() {
            return new LocalGatewayMetaState(version, metaData);
        }

        public static void toXContent(LocalGatewayMetaState state, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject("state");

            builder.field("version", state.version());
            MetaData.Builder.toXContent(state.metaData(), builder, params);

            builder.endObject();
        }

        public static LocalGatewayMetaState fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder();

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                // no data...
                return builder.build();
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("meta-data".equals(currentFieldName)) {
                        builder.metaData = MetaData.Builder.fromXContent(parser);
                    }
                } else if (token.isValue()) {
                    if ("version".equals(currentFieldName)) {
                        builder.version = parser.longValue();
                    }
                }
            }

            return builder.build();
        }

        public static LocalGatewayMetaState readFrom(StreamInput in) throws IOException {
            LocalGatewayMetaState.Builder builder = new Builder();
            builder.version = in.readLong();
            builder.metaData = MetaData.Builder.readFrom(in);
            return builder.build();
        }

        public static void writeTo(LocalGatewayMetaState state, StreamOutput out) throws IOException {
            out.writeLong(state.version());
            MetaData.Builder.writeTo(state.metaData(), out);
        }
    }

}
