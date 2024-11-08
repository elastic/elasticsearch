/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.errorquery;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A test query that can simulate errors and warnings when executing a shard request.
 * It allows specifying errors or warnings on a per-index basis and on a per-shard basis
 * within an index. If shards are not specified, then the error or warning action will
 * occur on all shards.
 *
 * To simulate longer running queries a stall (sleep) time can be added to each
 * indices entry that specifies how long to sleep before doing a search or
 * throwing an Exception.
 *
 * This can also be used for CCS testing. Example:
 * <pre>
 *    POST blogs,remote*:blogs/_async_search?ccs_minimize_roundtrips=true
 *    {
 *      "size": 0,
 *      "query": {
 *        "error_query": {
 *          "indices": [
 *          {
 *            "name": "*",
 *            "shard_ids": [0],
 *            "error_type": "exception",
 *            "message": "local cluster exception"
 *          },
 *          {
 *            "stall_time_seconds": 1,
 *            "name": "remote2:*",
 *            "error_type": "exception",
 *            "message": "remote2 exception"
 *          },
 *          {
 *            "stall_time_seconds": 7,
 *             "name": "remote1:blogs",
 *             "error_type": "none"
 *          }
 *          ]
 *        }
 *      },
 *      "aggs": {
 *        "indexgroup": {
 *          "terms": {
 *            "field": "_index"
 *          }
 *        }
 *      }
 *    }
 *  </pre>
 */

public class ErrorQueryBuilder extends AbstractQueryBuilder<ErrorQueryBuilder> {
    public static final String NAME = "error_query";

    private List<IndexError> indices;

    public ErrorQueryBuilder(List<IndexError> indices) {
        this.indices = Objects.requireNonNull(indices);
    }

    public ErrorQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.indices = in.readCollectionAsList(IndexError::new);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeCollection(indices);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        // Disable the request cache
        context.nowInMillis();

        IndexError error = null;
        for (IndexError index : indices) {
            if (context.indexMatches(index.getIndexName())) {
                error = index;
                break;
            }
        }
        if (error == null) {
            return new MatchAllDocsQuery();
        }

        return new ErrorQuery(error, context);
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<ErrorQueryBuilder, String> PARSER = new ConstructingObjectParser<>(NAME, false, (args, name) -> {
        ErrorQueryBuilder q = new ErrorQueryBuilder((List<IndexError>) args[0]);
        final float boost = args[1] == null ? DEFAULT_BOOST : (Float) args[1];
        final String queryName = (String) args[2];
        q.boost = boost;
        q.queryName = queryName;
        return q;
    });

    static {
        PARSER.declareObjectArray(constructorArg(), IndexError.PARSER, new ParseField("indices"));
        PARSER.declareFloat(ConstructingObjectParser.optionalConstructorArg(), BOOST_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), NAME_FIELD);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startArray("indices");
        for (IndexError indexError : indices) {
            builder.startObject();
            indexError.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    @Override
    protected boolean doEquals(ErrorQueryBuilder other) {
        return Objects.equals(indices, other.indices);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(indices);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    /**
     * ErrorQuery uses MatchAllDocsQuery when doing searches.
     * It can optionally add warnings, throw exceptions and sleep for specified "stall times"
     * based on the information in the provided IndexError class.
     */
    static class ErrorQuery extends Query {
        private final IndexError indexError;
        private volatile boolean sleepCompleted;
        private final MatchAllDocsQuery matchAllQuery;

        ErrorQuery(IndexError error, SearchExecutionContext context) {
            this.indexError = error;
            this.sleepCompleted = false;
            this.matchAllQuery = new MatchAllDocsQuery();

            if (error.getShardIds() != null) {
                boolean match = false;
                for (int shardId : error.getShardIds()) {
                    if (context.getShardId() == shardId) {
                        match = true;
                        break;
                    }
                }
                if (match == false) {
                    return;
                }
            }
            final String header = "[" + context.index().getName() + "][" + context.getShardId() + "]";
            if (error.getErrorType() == IndexError.ERROR_TYPE.WARNING) {
                HeaderWarning.addWarning(header + " " + error.getMessage());
            } else if (error.getErrorType() == IndexError.ERROR_TYPE.EXCEPTION) {
                if (indexError.getStallTimeSeconds() > 0) {
                    sleep(indexError.getStallTimeSeconds() * 1000L);
                }
                throw new RuntimeException(header + " " + error.getMessage());
            }
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
            if (indexError.getStallTimeSeconds() > 0 && sleepCompleted == false) {
                sleep(indexError.getStallTimeSeconds() * 1000L);
                sleepCompleted = true;
            }
            return matchAllQuery.createWeight(searcher, scoreMode, boost);
        }

        @Override
        public String toString(String field) {
            return "ErrorQuery MatchAll *:*";
        }

        @Override
        public boolean equals(Object o) {
            return sameClassAs(o);
        }

        @Override
        public int hashCode() {
            return classHash();
        }

        @Override
        public void visit(QueryVisitor visitor) {
            matchAllQuery.visit(visitor);
        }
    }
}
