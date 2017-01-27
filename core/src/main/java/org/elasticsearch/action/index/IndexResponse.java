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

package org.elasticsearch.action.index;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response of an index operation,
 *
 * @see org.elasticsearch.action.index.IndexRequest
 * @see org.elasticsearch.client.Client#index(IndexRequest)
 */
public class IndexResponse extends DocWriteResponse {

    private static final String CREATED = "created";

    public IndexResponse() {
    }

    public IndexResponse(ShardId shardId, String type, String id, long seqNo, long version, boolean created) {
        super(shardId, type, id, seqNo, version, created ? Result.CREATED : Result.UPDATED);
    }

    @Override
    public RestStatus status() {
        return result == Result.CREATED ? RestStatus.CREATED : super.status();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("IndexResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",seqNo=").append(getSeqNo());
        builder.append(",shards=").append(Strings.toString(getShardInfo()));
        return builder.append("]").toString();
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerToXContent(builder, params);
        builder.field(CREATED, result == Result.CREATED);
        return builder;
    }

    public static IndexResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);

        ParsingContext context = new ParsingContext();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            parseXContentFields(parser, context);
        }
        return fromParsingContext(context);
    }

    /**
     * Parse the current token and update the parsing context appropriately.
     */
    public static void parseXContentFields(XContentParser parser, ParsingContext context) throws IOException {
        XContentParser.Token token = parser.currentToken();
        String currentFieldName = parser.currentName();

        if (CREATED.equals(currentFieldName)) {
            if (token.isValue()) {
                context.setCreated(parser.booleanValue());
            }
        } else {
            DocWriteResponse.parseInnerToXContent(parser, context);
        }
    }

    /**
     * Create a {@link IndexResponse} from a parsing context.
     */
    public static IndexResponse fromParsingContext(ParsingContext context) {
        IndexResponse indexResponse = new IndexResponse(context.getShardId(), context.getType(), context.getId(),
                context.getSeqNo(), context.getVersion(), context.isCreated());
        indexResponse.setForcedRefresh(context.isForcedRefresh());
        if (context.getShardInfo() != null) {
            indexResponse.setShardInfo(context.getShardInfo());
        }
        return indexResponse;
    }

    public static class ParsingContext extends DocWriteResponse.ParsingContext {

        private boolean created = false;

        public boolean isCreated() {
            return created;
        }

        public void setCreated(boolean created) {
            this.created = created;
        }
    }
}
