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

package org.elasticsearch.client;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.index.seqno.SequenceNumbers;

import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * An object representing a response to the deletion of a document.
 * @see DeleteRequest
 */
public final class DeleteResponse {

    static final ConstructingObjectParser<DeleteResponse, Void> PARSER = new ConstructingObjectParser<>("delete_response",
            a -> new DeleteResponse((String) a[0], (String) a[1], (Result) a[2], (ShardInfo) a[3], (Long) a[4], (Long) a[5], (Long) a[6]));
    static {
        PARSER.declareString(constructorArg(), new ParseField("_index"));
        PARSER.declareString(constructorArg(), new ParseField("_id"));
        PARSER.declareField(constructorArg(), (parser, c) -> {
            switch (parser.text()) {
            case "deleted":
                return Result.DELETED;
            case "not_found":
                return Result.NOT_FOUND;
            default:
                throw new XContentParseException(parser.getTokenLocation(), "Unexpected _result value: [" + parser.text() + "]");
            }
        }, new ParseField("result"), ValueType.STRING);
        PARSER.declareObject(constructorArg(), ShardInfo.PARSER, new ParseField(ShardInfo.SHARDS_FIELD));
        PARSER.declareLong(constructorArg(), new ParseField("_version"));
        PARSER.declareLong(constructorArg(), new ParseField("_primary_term"));
        PARSER.declareLong(constructorArg(), new ParseField("_seq_no"));
    }

    private final String index, id;
    private final Result result;
    private final long version;
    private final long primaryTerm;
    private final long seqNo;
    private final ShardInfo shardInfo;

    private DeleteResponse(String index, String id, Result result, ShardInfo shardInfo,
            long version, long primaryTerm, long seqNo) {
        this.index = Objects.requireNonNull(index, "_index may not be null");
        this.id = Objects.requireNonNull(id, "_id may not be null");
        this.result = Objects.requireNonNull(result, "_result may not be null");
        this.shardInfo = Objects.requireNonNull(shardInfo, "_shards may not be null");
        this.version = version;
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
    }

    /**
     * Return whether the document has been deleted or whether it already did not exist.
     */
    public Result getResult() {
        return result;
    }

    /**
     * Return the index that the deleted document belonged to.
     */
    public String getIndex() {
        return index;
    }

    /**
     * Return the id of the deleted document.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the current version of the deleted document or {@link Versions#MATCH_ANY} if the document didn't exist.
     */
    public long getVersion() {
        return version;
    }

    /**
     * Returns the sequence number assigned for this change. Returns {@link SequenceNumbers#UNASSIGNED_SEQ_NO}
     * if the document didn't exist.
     */
    public long getSeqNo() {
        return seqNo;
    }

    /**
     * Return the primary term of the deleted document.
     */
    public long getPrimaryTerm() {
        return primaryTerm;
    }

    /**
     * Return information about how many shards processed the request and potential failures.
     */
    public ShardInfo getShardInfo() {
        return shardInfo;
    }
}
