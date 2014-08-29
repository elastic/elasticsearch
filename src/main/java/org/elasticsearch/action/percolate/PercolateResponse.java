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
package org.elasticsearch.action.percolate;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.percolator.PercolatorService;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.highlight.HighlightField;

import java.io.IOException;
import java.util.*;

/**
 * Encapsulates the response of a percolator request.
 */
public class PercolateResponse extends BroadcastOperationResponse implements Iterable<PercolateResponse.Match>, ToXContent {

    public static final Match[] EMPTY = new Match[0];

    private long tookInMillis;
    private Match[] matches;
    private long count;
    private InternalAggregations aggregations;

    PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures,
                             Match[] matches, long count, long tookInMillis, InternalAggregations aggregations) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
        this.matches = matches;
        this.count = count;
        this.aggregations = aggregations;
    }

    PercolateResponse(int totalShards, int successfulShards, int failedShards, List<ShardOperationFailedException> shardFailures, long tookInMillis, Match[] matches) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.tookInMillis = tookInMillis;
        this.matches = matches;
    }

    PercolateResponse() {
    }

    /**
     * How long the percolate took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the percolate took in milliseconds.
     */
    public long getTookInMillis() {
        return tookInMillis;
    }

    /**
     * @return The queries that match with the document being percolated. This can return <code>null</code> if th.
     */
    public Match[] getMatches() {
        return this.matches;
    }

    /**
     * @return The total number of queries that have matched with the document being percolated.
     */
    public long getCount() {
        return count;
    }

    /**
     * @return Any aggregations that has been executed on the query metadata. This can return <code>null</code>.
     */
    public InternalAggregations getAggregations() {
        return aggregations;
    }

    @Override
    public Iterator<Match> iterator() {
        return Arrays.asList(matches).iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.TOOK, tookInMillis);
        RestActions.buildBroadcastShardsHeader(builder, this);

        builder.field(Fields.TOTAL, count);
        if (matches != null) {
            builder.startArray(Fields.MATCHES);
            boolean justIds = "ids".equals(params.param("percolate_format"));
            if (justIds) {
                for (PercolateResponse.Match match : matches) {
                    builder.value(match.getId());
                }
            } else {
                for (PercolateResponse.Match match : matches) {
                    builder.startObject();
                    builder.field(Fields._INDEX, match.getIndex());
                    builder.field(Fields._ID, match.getId());
                    float score = match.getScore();
                    if (score != PercolatorService.NO_SCORE) {
                        builder.field(Fields._SCORE, match.getScore());
                    }
                    if (match.getHighlightFields() != null) {
                        builder.startObject(Fields.HIGHLIGHT);
                        for (HighlightField field : match.getHighlightFields().values()) {
                            builder.field(field.name());
                            if (field.fragments() == null) {
                                builder.nullValue();
                            } else {
                                builder.startArray();
                                for (Text fragment : field.fragments()) {
                                    builder.value(fragment);
                                }
                                builder.endArray();
                            }
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }
            builder.endArray();
        }
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tookInMillis = in.readVLong();
        count = in.readVLong();
        int size = in.readVInt();
        if (size != -1) {
            matches = new Match[size];
            for (int i = 0; i < size; i++) {
                matches[i] = new Match();
                matches[i].readFrom(in);
            }
        }
        aggregations = InternalAggregations.readOptionalAggregations(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(tookInMillis);
        out.writeVLong(count);
        if (matches == null) {
            out.writeVInt(-1);
        } else {
            out.writeVInt(matches.length);
            for (Match match : matches) {
                match.writeTo(out);
            }
        }
        out.writeOptionalStreamable(aggregations);
    }

    /**
     * Represents a query that has matched with the document that was percolated.
     */
    public static class Match implements Streamable {

        private Text index;
        private Text id;
        private float score;
        private Map<String, HighlightField> hl;

        /**
         * Constructor only for internal usage.
         */
        public Match(Text index, Text id, float score, Map<String, HighlightField> hl) {
            this.id = id;
            this.score = score;
            this.index = index;
            this.hl = hl;
        }

        /**
         * Constructor only for internal usage.
         */
        public Match(Text index, Text id, float score) {
            this.id = id;
            this.score = score;
            this.index = index;
        }

        Match() {
        }

        /**
         * @return The index that the matched percolator query resides in.
         */
        public Text getIndex() {
            return index;
        }

        /**
         * @return The id of the matched percolator query.
         */
        public Text getId() {
            return id;
        }

        /**
         * @return If in the percolate request a query was specified this returns the score representing how well that
         * query matched on the metadata associated with the matching query otherwise {@link Float#NaN} is returned.
         */
        public float getScore() {
            return score;
        }

        /**
         * @return If highlighting was specified in the percolate request the this returns highlight snippets for each
         * matching field in the document being percolated based on this query otherwise <code>null</code> is returned.
         */
        @Nullable
        public Map<String, HighlightField> getHighlightFields() {
            return hl;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            id = in.readText();
            index = in.readText();
            score = in.readFloat();
            int size = in.readVInt();
            if (size > 0) {
                hl = new HashMap<>(size);
                for (int j = 0; j < size; j++) {
                    hl.put(in.readString(), HighlightField.readHighlightField(in));
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeText(id);
            out.writeText(index);
            out.writeFloat(score);
            if (hl != null) {
                out.writeVInt(hl.size());
                for (Map.Entry<String, HighlightField> entry : hl.entrySet()) {
                    out.writeString(entry.getKey());
                    entry.getValue().writeTo(out);
                }
            } else {
                out.writeVInt(0);
            }
        }
    }

    static final class Fields {
        static final XContentBuilderString TOOK = new XContentBuilderString("took");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MATCHES = new XContentBuilderString("matches");
        static final XContentBuilderString _INDEX = new XContentBuilderString("_index");
        static final XContentBuilderString _ID = new XContentBuilderString("_id");
        static final XContentBuilderString _SCORE = new XContentBuilderString("_score");
        static final XContentBuilderString HIGHLIGHT = new XContentBuilderString("highlight");
    }

}
