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

package org.elasticsearch.index.reindex;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.index.reindex.BulkByScrollTask.Status;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.reindex.ScrollableHitSource.SearchFailure;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueNanos;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Response used for actions that index many documents using a scroll request.
 */
public class BulkByScrollResponse extends ActionResponse implements ToXContentFragment {
    private TimeValue took;
    private BulkByScrollTask.Status status;
    private List<Failure> bulkFailures;
    private List<ScrollableHitSource.SearchFailure> searchFailures;
    private boolean timedOut;

    private static final String TOOK_FIELD = "took";
    private static final String TIMED_OUT_FIELD = "timed_out";
    private static final String FAILURES_FIELD = "failures";

    @SuppressWarnings("unchecked")
    private static final ObjectParser<BulkByScrollResponseBuilder, Void> PARSER =
        new ObjectParser<>(
            "bulk_by_scroll_response",
            true,
            BulkByScrollResponseBuilder::new
        );
    static {
        PARSER.declareLong(BulkByScrollResponseBuilder::setTook, new ParseField(TOOK_FIELD));
        PARSER.declareBoolean(BulkByScrollResponseBuilder::setTimedOut, new ParseField(TIMED_OUT_FIELD));
        PARSER.declareObjectArray(
            BulkByScrollResponseBuilder::setFailures, (p, c) -> parseFailure(p), new ParseField(FAILURES_FIELD)
        );
        // since the result of BulkByScrollResponse.Status are mixed we also parse that in this
        Status.declareFields(PARSER);
    }

    public BulkByScrollResponse(StreamInput in) throws IOException {
        super(in);
        took = in.readTimeValue();
        status = new BulkByScrollTask.Status(in);
        bulkFailures = in.readList(Failure::new);
        searchFailures = in.readList(ScrollableHitSource.SearchFailure::new);
        timedOut = in.readBoolean();
    }

    public BulkByScrollResponse(TimeValue took, BulkByScrollTask.Status status, List<Failure> bulkFailures,
                                List<ScrollableHitSource.SearchFailure> searchFailures, boolean timedOut) {
        this.took = took;
        this.status = requireNonNull(status, "Null status not supported");
        this.bulkFailures = bulkFailures;
        this.searchFailures = searchFailures;
        this.timedOut = timedOut;
    }

    public BulkByScrollResponse(Iterable<BulkByScrollResponse> toMerge, @Nullable String reasonCancelled) {
        long mergedTook = 0;
        List<BulkByScrollTask.StatusOrException> statuses = new ArrayList<>();
        bulkFailures = new ArrayList<>();
        searchFailures = new ArrayList<>();
        for (BulkByScrollResponse response : toMerge) {
            mergedTook = max(mergedTook, response.getTook().nanos());
            statuses.add(new BulkByScrollTask.StatusOrException(response.status));
            bulkFailures.addAll(response.getBulkFailures());
            searchFailures.addAll(response.getSearchFailures());
            timedOut |= response.isTimedOut();
        }
        took = timeValueNanos(mergedTook);
        status = new BulkByScrollTask.Status(statuses, reasonCancelled);
    }

    public TimeValue getTook() {
        return took;
    }

    public BulkByScrollTask.Status getStatus() {
        return status;
    }

    public long getCreated() {
        return status.getCreated();
    }

    public long getTotal() {
        return status.getTotal();
    }

    public long getDeleted() {
        return status.getDeleted();
    }

    public long getUpdated() {
        return status.getUpdated();
    }

    public int getBatches() {
        return status.getBatches();
    }

    public long getVersionConflicts() {
        return status.getVersionConflicts();
    }

    public long getNoops() {
        return status.getNoops();
    }

    /**
     * The reason that the request was canceled or null if it hasn't been.
     */
    public String getReasonCancelled() {
        return status.getReasonCancelled();
    }

    /**
     * The number of times that the request had retry bulk actions.
     */
    public long getBulkRetries() {
        return status.getBulkRetries();
    }

    /**
     * The number of times that the request had retry search actions.
     */
    public long getSearchRetries() {
        return status.getSearchRetries();
    }

    /**
     * All of the bulk failures. Version conflicts are only included if the request sets abortOnVersionConflict to true (the default).
     */
    public List<Failure> getBulkFailures() {
        return bulkFailures;
    }

    /**
     * All search failures.
     */
    public List<ScrollableHitSource.SearchFailure> getSearchFailures() {
        return searchFailures;
    }

    /**
     * Did any of the sub-requests that were part of this request timeout?
     */
    public boolean isTimedOut() {
        return timedOut;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeTimeValue(took);
        status.writeTo(out);
        out.writeList(bulkFailures);
        out.writeList(searchFailures);
        out.writeBoolean(timedOut);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(TOOK_FIELD, took.millis());
        builder.field(TIMED_OUT_FIELD, timedOut);
        status.innerXContent(builder, params);
        builder.startArray("failures");
        for (Failure failure: bulkFailures) {
            builder.startObject();
            failure.toXContent(builder, params);
            builder.endObject();
        }
        for (ScrollableHitSource.SearchFailure failure: searchFailures) {
            failure.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public static BulkByScrollResponse fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null).buildResponse();
    }

    private static Object parseFailure(XContentParser parser) throws IOException {
       ensureExpectedToken(Token.START_OBJECT, parser.currentToken(), parser::getTokenLocation);
       Token token;
       String index = null;
       String type = null;
       String id = null;
       Integer status = null;
       Integer shardId = null;
       String nodeId = null;
       ElasticsearchException bulkExc = null;
       ElasticsearchException searchExc = null;
       while ((token = parser.nextToken()) != Token.END_OBJECT) {
           ensureExpectedToken(Token.FIELD_NAME, token, parser::getTokenLocation);
           String name = parser.currentName();
           token = parser.nextToken();
           if (token == Token.START_ARRAY) {
               parser.skipChildren();
           } else if (token == Token.START_OBJECT) {
               switch (name) {
                   case SearchFailure.REASON_FIELD:
                       searchExc = ElasticsearchException.fromXContent(parser);
                       break;
                   case Failure.CAUSE_FIELD:
                       bulkExc = ElasticsearchException.fromXContent(parser);
                       break;
                   default:
                       parser.skipChildren();
               }
           } else if (token == Token.VALUE_STRING) {
               switch (name) {
                   // This field is the same as SearchFailure.index
                   case Failure.INDEX_FIELD:
                       index = parser.text();
                       break;
                   case Failure.TYPE_FIELD:
                       type = parser.text();
                       break;
                   case Failure.ID_FIELD:
                       id = parser.text();
                       break;
                   case SearchFailure.NODE_FIELD:
                       nodeId = parser.text();
                       break;
                   default:
                       // Do nothing
                       break;
               }
           } else if (token == Token.VALUE_NUMBER) {
               switch (name) {
                   case Failure.STATUS_FIELD:
                       status = parser.intValue();
                       break;
                   case SearchFailure.SHARD_FIELD:
                       shardId = parser.intValue();
                       break;
                   default:
                       // Do nothing
                       break;
               }
           }
       }
       if (bulkExc != null) {
           return new Failure(index, type, id, bulkExc, RestStatus.fromCode(status));
       } else if (searchExc != null) {
           if (status == null) {
               return new SearchFailure(searchExc, index, shardId, nodeId);
           } else {
               return new SearchFailure(searchExc, index, shardId, nodeId, RestStatus.fromCode(status));
           }
       } else {
           throw new ElasticsearchParseException("failed to parse failures array. At least one of {reason,cause} must be present");
       }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(getClass().getSimpleName()).append("[");
        builder.append("took=").append(took).append(',');
        builder.append("timed_out=").append(timedOut).append(',');
        status.innerToString(builder);
        builder.append(",bulk_failures=").append(getBulkFailures().subList(0, min(3, getBulkFailures().size())));
        builder.append(",search_failures=").append(getSearchFailures().subList(0, min(3, getSearchFailures().size())));
        return builder.append(']').toString();
    }
}
