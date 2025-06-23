/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A match all QueryBuilder that sleeps for a specified amount of time at various
 * points in the query process.
 *
 * This QueryBuilder is useful in tests that need a slow running query, such as when
 * you are trying to have a query timeout.
 *
 * The sleep can be specified to happen on all indices or only on a specified index.
 * After sleeping (if at all), it performs a MatchAll query.
 */
public class SlowRunningQueryBuilder extends AbstractQueryBuilder<SlowRunningQueryBuilder> {

    public static final String NAME = "slow";

    private long sleepTime;
    private String index;

    /**
     * Sleep for sleepTime millis on all indexes
     * @param sleepTime
     */
    public SlowRunningQueryBuilder(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    /**
     * Sleep for sleepTime millis but only on the specified index
     * @param sleepTime
     */
    public SlowRunningQueryBuilder(long sleepTime, String index) {
        this.sleepTime = sleepTime;
        this.index = index;
    }

    public SlowRunningQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.sleepTime = in.readLong();
        this.index = in.readOptionalString();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(sleepTime);
        out.writeOptionalString(index);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private void sleep() {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {}
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        final Query delegate = Queries.newMatchAllQuery();
        return new Query() {
            @Override
            public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
                if (index == null || context.index().getName().equals(index)) {
                    sleep();
                }
                return delegate.createWeight(searcher, scoreMode, boost);
            }

            @Override
            public String toString(String field) {
                return delegate.toString(field);
            }

            @Override
            public boolean equals(Object obj) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }

            @Override
            public void visit(QueryVisitor visitor) {
                visitor.visitLeaf(this);
            }
        };
    }

    @Override
    protected boolean doEquals(SlowRunningQueryBuilder other) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }
}
