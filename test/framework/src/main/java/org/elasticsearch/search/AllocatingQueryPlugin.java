/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.query.LeafQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.test.ESTestCase.safeAwait;

/**
 * A query that matches all documents and charges a fixed number of bytes while its search context is open. Tests use it
 * as a known allocation. While a {@link Pause} is open, the query waits when its weight is created.
 */
public class AllocatingQueryPlugin extends Plugin implements SearchPlugin {

    private static volatile Pause pause;

    /**
     * Opens a pause: allocating queries wait when their weight is created, until it is closed.
     */
    public static Pause pause() {
        Pause opened = new Pause();
        pause = opened;
        return opened;
    }

    /**
     * Holds the allocating queries that reach it until it is closed.
     */
    public static final class Pause implements Releasable {
        private final CountDownLatch reached = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);

        /**
         * Waits until a query reaches the pause.
         */
        public void awaitReached() {
            safeAwait(reached);
        }

        private void hold() {
            reached.countDown();
            safeAwait(closed);
        }

        @Override
        public void close() {
            pause = null;
            closed.countDown();
        }
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return List.of(new QuerySpec<>(AllocatingQueryBuilder.NAME, AllocatingQueryBuilder::new, AllocatingQueryBuilder::fromXContent));
    }

    public static class AllocatingQueryBuilder extends LeafQueryBuilder<AllocatingQueryBuilder> {
        public static final String NAME = "allocating";

        private final long bytes;

        public AllocatingQueryBuilder(long bytes) {
            this.bytes = bytes;
        }

        public AllocatingQueryBuilder(StreamInput in) throws IOException {
            super(in);
            this.bytes = in.readVLong();
        }

        @Override
        protected void doWriteTo(StreamOutput out) throws IOException {
            out.writeVLong(bytes);
        }

        @Override
        protected void doXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(NAME).field("bytes", bytes).endObject();
        }

        public static AllocatingQueryBuilder fromXContent(XContentParser parser) throws IOException {
            long bytes = 0;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token.isValue()) {
                    bytes = parser.longValue();
                }
            }
            return new AllocatingQueryBuilder(bytes);
        }

        @Override
        protected Query doToQuery(SearchExecutionContext context) {
            context.addCircuitBreakerMemory(bytes, NAME);
            return new PausingQuery();
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(bytes);
        }

        @Override
        protected boolean doEquals(AllocatingQueryBuilder other) {
            return bytes == other.bytes;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.zero();
        }
    }

    /**
     * Matches all documents, and waits when its weight is created while a {@link Pause} is open.
     */
    private static final class PausingQuery extends Query {
        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            Pause current = pause;
            if (current != null) {
                current.hold();
            }
            return Queries.ALL_DOCS_INSTANCE.createWeight(searcher, scoreMode, boost);
        }

        @Override
        public String toString(String field) {
            return "pausing";
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public boolean equals(Object obj) {
            return sameClassAs(obj);
        }

        @Override
        public int hashCode() {
            return classHash();
        }
    }
}
