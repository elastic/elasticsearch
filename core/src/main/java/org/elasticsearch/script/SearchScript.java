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
import org.apache.lucene.util.SetOnce;
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
 *     <li>Constructing using a {@link Factory}</li>
 *     <li>Creating a duplicate {@link SearchScript} bound to a segment using {@link #forSegment(LeafReaderContext)}</li>
 *     <li>Calling {@link #setDocument(int)} to indicate which document in the segment the script should be run for next</li>
 *     <li>Calling one of the {@code run} methods: {@link #run()}, {@link #runAsDouble()}, or {@link #runAsLong()}</li>
 * </ol>
 */
public abstract class SearchScript implements ScorerAware, ExecutableScript, Cloneable {

    /** The generic runtime parameters for the script. */
    public final Map<String, Object> params;

    /** A lookup for the index this script will operate on. */
    public final SearchLookup lookup;

    /** A leaf lookup for the bound segment this script will operate on. */
    private SetOnce<LeafSearchLookup> leafLookup = new SetOnce<>();

    /** A scorer that will return the score for the current document when the script is run. */
    private SetOnce<Scorer> scorer = new SetOnce<>();

    /**
     * Construct an unbound search script.
     * Calling any methods except {@link #forSegment(LeafReaderContext)} on the returned instance is undefined.
     */
    public SearchScript(Map<String, Object> params, SearchLookup lookup) {
        this.params = params;
        this.lookup = lookup;
    }

    /**
     * Constructs a copy of this script that is bound to the given segment.
     *
     * @param leaf The reader context for a Lucene segment
     * @return A new {@link SearchScript} instance
     */
    public SearchScript forSegment(LeafReaderContext leaf) {
        try {
            SearchScript clone = (SearchScript) super.clone();
            if (lookup != null) {
                clone.leafLookup.set(lookup.getLeafSearchLookup(leaf));
            }
            return clone;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("impossible", e);
        }
    }

    /** The leaf lookup for the bound segment. */
    protected final LeafSearchLookup getLeafLookup() {
        return leafLookup.get();
    }

    /** The doc lookup for the bound segment. */
    public final LeafDocLookup getDoc() {
        return leafLookup.get() == null ? null : leafLookup.get().doc();
    }

    /** Set the current document to run the script on next. */
    public void setDocument(int docid) {
        if (leafLookup.get() != null) {
            leafLookup.get().setDocument(docid);
        }
    }

    @Override
    public void setScorer(Scorer scorer) {
        this.scorer.set(scorer);
    }

    /** Return the score of the current document. */
    public double getScore() {
        if (needsScores() == false || scorer.get() == null) {
            return 0.0d;
        }
        try {
            return scorer.get().score();
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

    /**
     * Indicates if document scores may be needed by this {@link SearchScript}.
     * 
     * @return {@code true} if scores are needed.
     */
    public abstract boolean needsScores();

    /** A factory to construct {@link SearchScript}. */
    public interface Factory {
        SearchScript newInstance(Map<String, Object> params, SearchLookup lookup);
    }

    /** The context used to compile {@link SearchScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("search", Factory.class);
    // TODO: remove aggs context when it has its own interface
    public static final ScriptContext<Factory> AGGS_CONTEXT = new ScriptContext<>("aggs", Factory.class);
}