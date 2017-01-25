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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

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

    /**
     * ConstructingObjectParser used to parse the {@link IndexResponse}. We use a ObjectParser here
     * because most fields are parsed by the parent abstract class {@link DocWriteResponse} and it's
     * not easy to parse part of the fields in the parent class and other fields in the children class
     * using the usual streamed parsing method.
     */
    private static final ConstructingObjectParser<IndexResponse, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(IndexResponse.class.getName(),
                args -> {
                    // index uuid and shard id are unknown and can't be parsed back for now.
                    ShardId shardId = new ShardId(new Index((String) args[0], IndexMetaData.INDEX_UUID_NA_VALUE), -1);
                    String type = (String) args[1];
                    String id = (String) args[2];
                    long version = (long) args[3];
                    ShardInfo shardInfo = (ShardInfo) args[5];
                    long seqNo = (args[6] != null) ? (long) args[6] : SequenceNumbersService.UNASSIGNED_SEQ_NO;
                    boolean created = (boolean) args[7];

                    IndexResponse indexResponse = new IndexResponse(shardId, type, id, seqNo, version, created);
                    indexResponse.setShardInfo(shardInfo);
                    return indexResponse;
                });
        DocWriteResponse.declareParserFields(PARSER);
        PARSER.declareBoolean(constructorArg(), new ParseField(CREATED));
    }

    public static IndexResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }
}
