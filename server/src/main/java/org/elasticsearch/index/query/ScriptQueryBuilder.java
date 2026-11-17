/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.DocValuesDocReader;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public class ScriptQueryBuilder extends LeafQueryBuilder<ScriptQueryBuilder> {
    public static final String NAME = "script";

    private final Script script;

    public ScriptQueryBuilder(Script script) {
        if (script == null) {
            throw new IllegalArgumentException("script cannot be null");
        }
        this.script = script;
    }

    /**
     * Read from a stream.
     */
    public ScriptQueryBuilder(StreamInput in) throws IOException {
        super(in);
        script = new Script(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        script.writeTo(out);
    }

    public Script script() {
        return this.script;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.startObject(NAME);
        builder.field(Script.SCRIPT_PARSE_FIELD.getPreferredName(), script);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static ScriptQueryBuilder fromXContent(XContentParser parser) throws IOException {
        // also, when caching, since its isCacheable is false, will result in loading all bit set...
        Script script = null;

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    script = Script.parse(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[script] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (Script.SCRIPT_PARSE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    script = Script.parse(parser);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[script] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (token != XContentParser.Token.START_ARRAY) {
                    throw new ParsingException(parser.getTokenLocation(), "[script] query does not support token [" + token + "]");
                }
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "[script] query does not support an array of scripts. Use a bool query with a clause per script instead."
                );
            }
        }

        if (script == null) {
            throw new ParsingException(parser.getTokenLocation(), "script must be provided with a [script] filter");
        }

        return new ScriptQueryBuilder(script).boost(boost).queryName(queryName);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[script] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }
        FilterScript.Factory factory = context.compile(script, FilterScript.CONTEXT);
        SearchLookup lookup = context.lookup();
        FilterScript.LeafFactory filterScript = factory.newFactory(script.getParams(), lookup);
        return new ScriptQuery(script, filterScript, lookup);
    }

    static class ScriptQuery extends Query {

        final Script script;
        final FilterScript.LeafFactory filterScript;
        final SearchLookup lookup;

        ScriptQuery(Script script, FilterScript.LeafFactory filterScript, SearchLookup lookup) {
            this.script = script;
            this.filterScript = filterScript;
            this.lookup = lookup;
        }

        @Override
        public String toString(String field) {
            StringBuilder buffer = new StringBuilder();
            buffer.append("ScriptQuery(");
            buffer.append(script);
            buffer.append(")");
            return buffer.toString();
        }

        @Override
        public boolean equals(Object obj) {
            if (sameClassAs(obj) == false) return false;
            ScriptQuery other = (ScriptQuery) obj;
            return Objects.equals(script, other.script);
        }

        @Override
        public int hashCode() {
            int h = classHash();
            h = 31 * h + script.hashCode();
            return h;
        }

        @Override
        public void visit(QueryVisitor visitor) {
            visitor.visitLeaf(this);
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
            final Runnable cancellationCheck = (searcher instanceof ContextIndexSearcher cis) ? cis::checkCancelled : null;
            return new ConstantScoreWeight(this, boost) {

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    DocIdSetIterator approximation = new ExpensiveMatchAllIterator(context.reader().maxDoc());
                    final FilterScript leafScript = filterScript.newInstance(new DocValuesDocReader(lookup, context));
                    leafScript._setCancellationCheck(cancellationCheck);
                    TwoPhaseIterator twoPhase = new TwoPhaseIterator(approximation) {

                        @Override
                        public boolean matches() throws IOException {
                            leafScript.setDocument(approximation.docID());
                            return leafScript.execute();
                        }

                        @Override
                        public float matchCost() {
                            // TODO: how can we compute this?
                            return 1000f;
                        }
                    };
                    Scorer scorer = new ConstantScoreScorer(score(), scoreMode, twoPhase);
                    return new DefaultScorerSupplier(scorer);
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    // TODO: Change this to true when we can assume that scripts are pure functions
                    // ie. the return value is always the same given the same conditions and may not
                    // depend on the current timestamp, other documents, etc.
                    return false;
                }
            };
        }

        /**
         * A match-all approximation whose {@link #cost()} is deliberately reported as higher than any
         * real iterator could report, including a worst-case {@link DocIdSetIterator#NO_MORE_DOCS}
         * estimate from another clause. Running the script in {@code matches()} is the most expensive
         * part of evaluating this query, so a conjunction with any other clause should always confirm
         * that clause's approximation first rather than treating this one as cheap to lead with just
         * because it happens to match every document.
         */
        private static final class ExpensiveMatchAllIterator extends FilterDocIdSetIterator {

            ExpensiveMatchAllIterator(int maxDoc) {
                super(DocIdSetIterator.all(maxDoc));
            }

            @Override
            public long cost() {
                return (long) DocIdSetIterator.NO_MORE_DOCS + 100;
            }
        }
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(script);
    }

    @Override
    protected boolean doEquals(ScriptQueryBuilder other) {
        return Objects.equals(script, other.script);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.zero();
    }
}
