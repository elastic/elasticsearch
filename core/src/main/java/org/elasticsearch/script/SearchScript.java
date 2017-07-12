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
package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.lucene.ScorerAware;
import org.elasticsearch.search.lookup.LeafDocLookup;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * A generic script used for per document use cases.
 *
 * Using a {@link SearchScript} works as follows:
 * <ol>
 *     <li>Construct a {@link Factory} using {@link ScriptService#compile(Script, ScriptContext)}</li>
 *     <li>Construct a {@link LeafFactory} for a an index using {@link Factory#newFactory(Map, SearchLookup)}</li>
 *     <li>Construct a {@link SearchScript} for a Lucene segment using {@link LeafFactory#newInstance(LeafReaderContext)}</li>
 *     <li>Call {@link #setDocument(int)} to indicate which document in the segment the script should be run for next</li>
 *     <li>Call one of the {@code run} methods: {@link #run()}, {@link #runAsDouble()}, or {@link #runAsLong()}</li>
 * </ol>
 */
public abstract class SearchScript implements ScorerAware, ExecutableScript {

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** A lookup for the index this script will operate on. */
    private final SearchLookup lookup;

    /** A leaf lookup for the bound segment this script will operate on. */
    private final LeafReaderContext leafContext;

    /** A leaf lookup for the bound segment this script will operate on. */
    private final LeafSearchLookup leafLookup;

    /** A scorer that will return the score for the current document when the script is run. */
    private Scorer scorer;

    public SearchScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        this.params = params;
        this.lookup = lookup;
        this.leafContext = leafContext;
        // TODO: remove leniency when painless does not implement SearchScript for executable script cases
        this.leafLookup = leafContext == null ? null : lookup.getLeafSearchLookup(leafContext);
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** The leaf lookup for the Lucene segment this script was created for. */
    protected final LeafSearchLookup getLeafLookup() {
        return leafLookup;
    }

    /** The leaf context for the Lucene segment this script was created for. */
    protected final LeafReaderContext getLeafContext() {
        return leafContext;
    }

    /** The doc lookup for the Lucene segment this script was created for. */
    public final LeafDocLookup getDoc() {
        // TODO: remove leniency when painless does not implement SearchScript for executable script cases
        return leafLookup == null ? null : leafLookup.doc();
    }

    /** Set the current document to run the script on next. */
    public void setDocument(int docid) {
        // TODO: remove leniency when painless does not implement SearchScript for executable script cases
        if (leafLookup != null) {
            leafLookup.setDocument(docid);
        }
    }

    @Override
    public void setScorer(Scorer scorer) {
        this.scorer = scorer;
    }

    /** Return the score of the current document. */
    public double getScore() {
        // TODO: remove leniency when painless does not implement SearchScript for executable script cases
        if (scorer == null) {
            return 0.0d;
        }
        try {
            return scorer.score();
        } catch (IOException e) {
            throw new ElasticsearchException("couldn't lookup score", e);
        }
    }

    /**
     * Sets per-document aggregation {@code _value}.
     * <p>
     * The default implementation just calls {@code setNextVar("_value", value)} but
     * some engines might want to handle this differently for better performance.
     * <p>
     * @param value per-document value, typically a String, Long, or Double
     */
    public void setNextAggregationValue(Object value) {
        setNextVar("_value", value);
    }

    @Override
    public void setNextVar(String field, Object value) {}

    /** Return the result as a long. This is used by aggregation scripts over long fields. */
    public long runAsLong() {
        throw new UnsupportedOperationException("runAsLong is not implemented");
    }

    @Override
    public Object run() {
        return runAsDouble();
    }

    /** Return the result as a double. This is the main use case of search script, used for document scoring. */
    public abstract double runAsDouble();

    /** A factory to construct {@link SearchScript} instances. */
    public interface LeafFactory {
        SearchScript newInstance(LeafReaderContext ctx) throws IOException;

        /**
         * Return {@code true} if the script needs {@code _score} calculated, or {@code false} otherwise.
         */
        boolean needs_score();
    }

    /** A factory to construct stateful {@link SearchScript} factories for a specific index. */
    public interface Factory {
        LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);
    }

    /** The context used to compile {@link SearchScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("search", Factory.class);
    // TODO: remove aggs context when it has its own interface
    public static final ScriptContext<Factory> AGGS_CONTEXT = new ScriptContext<>("aggs", Factory.class);
}