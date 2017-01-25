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

package org.elasticsearch.action.delete;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
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
 * The response of the delete action.
 *
 * @see org.elasticsearch.action.delete.DeleteRequest
 * @see org.elasticsearch.client.Client#delete(DeleteRequest)
 */
public class DeleteResponse extends DocWriteResponse {

    private static final String FOUND = "found";

    public DeleteResponse() {

    }

    public DeleteResponse(ShardId shardId, String type, String id, long seqNo, long version, boolean found) {
        super(shardId, type, id, seqNo, version, found ? Result.DELETED : Result.NOT_FOUND);
    }

    @Override
    public RestStatus status() {
        return result == Result.DELETED ? super.status() : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FOUND, result == Result.DELETED);
        super.innerToXContent(builder, params);
        return builder;
    }

    private static final ConstructingObjectParser<DeleteResponse, Void> PARSER;
    static {
        PARSER = new ConstructingObjectParser<>(DeleteResponse.class.getName(),
            args -> {
                // index uuid and shard id are unknown and can't be parsed back for now.
                ShardId shardId = new ShardId(new Index((String) args[0], IndexMetaData.INDEX_UUID_NA_VALUE), -1);
                String type = (String) args[1];
                String id = (String) args[2];
                long version = (long) args[3];
                ShardInfo shardInfo = (ShardInfo) args[5];
                long seqNo = (args[6] != null) ? (long) args[6] : SequenceNumbersService.UNASSIGNED_SEQ_NO;
                boolean found = (boolean) args[7];
                DeleteResponse deleteResponse = new DeleteResponse(shardId, type, id, seqNo, version, found);
                deleteResponse.setShardInfo(shardInfo);
                return deleteResponse;
            });
        DocWriteResponse.declareParserFields(PARSER);
        PARSER.declareBoolean(constructorArg(), new ParseField(FOUND));
    }

    public static DeleteResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DeleteResponse[");
        builder.append("index=").append(getIndex());
        builder.append(",type=").append(getType());
        builder.append(",id=").append(getId());
        builder.append(",version=").append(getVersion());
        builder.append(",result=").append(getResult().getLowercase());
        builder.append(",shards=").append(getShardInfo());
        return builder.append("]").toString();
    }
}
