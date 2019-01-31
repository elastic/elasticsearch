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

package org.elasticsearch.search.profile.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.profile.ProfileResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A container class to hold the profile results for a single shard in the request.
 * Contains a list of query profiles, a collector tree and a total rewrite tree.
 */
public final class QueryProfileShardResult implements Writeable, ToXContentObject {

    public static final String COLLECTOR = "collector";
    public static final String REWRITE_TIME = "rewrite_time";
    public static final String QUERY_ARRAY = "query";

    private final List<ProfileResult> queryProfileResults;

    private final CollectorResult profileCollector;

    private final long rewriteTime;

    public QueryProfileShardResult(List<ProfileResult> queryProfileResults, long rewriteTime,
                              CollectorResult profileCollector) {
        assert(profileCollector != null);
        this.queryProfileResults = queryProfileResults;
        this.profileCollector = profileCollector;
        this.rewriteTime = rewriteTime;
    }

    /**
     * Read from a stream.
     */
    public QueryProfileShardResult(StreamInput in) throws IOException {
        int profileSize = in.readVInt();
        queryProfileResults = new ArrayList<>(profileSize);
        for (int j = 0; j < profileSize; j++) {
            queryProfileResults.add(new ProfileResult(in));
        }

        profileCollector = new CollectorResult(in);
        rewriteTime = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(queryProfileResults.size());
        for (ProfileResult p : queryProfileResults) {
            p.writeTo(out);
        }
        profileCollector.writeTo(out);
        out.writeLong(rewriteTime);
    }


    public List<ProfileResult> getQueryResults() {
        return Collections.unmodifiableList(queryProfileResults);
    }

    public long getRewriteTime() {
        return rewriteTime;
    }

    public CollectorResult getCollectorResult() {
        return profileCollector;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(QUERY_ARRAY);
        for (ProfileResult p : queryProfileResults) {
            p.toXContent(builder, params);
        }
        builder.endArray();
        builder.field(REWRITE_TIME, rewriteTime);
        builder.startArray(COLLECTOR);
        profileCollector.toXContent(builder, params);
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static QueryProfileShardResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        String currentFieldName = null;
        List<ProfileResult> queryProfileResults = new ArrayList<>();
        long rewriteTime = 0;
        CollectorResult collector = null;
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (REWRITE_TIME.equals(currentFieldName)) {
                    rewriteTime = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (QUERY_ARRAY.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        queryProfileResults.add(ProfileResult.fromXContent(parser));
                    }
                } else if (COLLECTOR.equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        collector = CollectorResult.fromXContent(parser);
                    }
                } else {
                    parser.skipChildren();
                }
            } else {
                parser.skipChildren();
            }
        }
        return new QueryProfileShardResult(queryProfileResults, rewriteTime, collector);
    }
}
