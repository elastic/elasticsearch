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

package org.elasticsearch.action.admin.indices.refresh;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The response of a refresh action.
 */
public class RefreshResponse extends BroadcastResponse implements ToXContentFragment {

    private static final ConstructingObjectParser<RefreshResponse, Void> PARSER = new ConstructingObjectParser<>("refresh",
        true, arg -> (RefreshResponse) arg[0]);

    static {
        ConstructingObjectParser<RefreshResponse, Void> shardsParser = new ConstructingObjectParser<>("_shards", true,
            arg -> new RefreshResponse((int) arg[0], (int) arg[1], (int) arg[2], null));
        shardsParser.declareInt(constructorArg(), new ParseField(Fields.TOTAL));
        shardsParser.declareInt(constructorArg(), new ParseField(Fields.SUCCESSFUL));
        shardsParser.declareInt(constructorArg(), new ParseField(Fields.FAILED));
        PARSER.declareObject(constructorArg(), shardsParser, new ParseField(Fields._SHARDS));
    }

    RefreshResponse() {
    }

    RefreshResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
    }

    public static RefreshResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
