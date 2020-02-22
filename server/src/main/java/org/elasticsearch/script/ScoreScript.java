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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.Version;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.DoubleSupplier;

/**
 * A script used for adjusting the score on a per document basis.
 */
public abstract class ScoreScript {

    /** A helper to take in an explanation from a script and turn it into an {@link org.apache.lucene.search.Explanation}  */
    public static class ExplanationHolder {
        private String description;

        /**
         * Explain the current score.
         *
         * @param description A textual description of how the score was calculated
         */
        public void set(String description) {
            this.description = description;
        }

        public Explanation get(double score, Explanation subQueryExplanation) {
            if (description == null) {
                return null;
            }
            if (subQueryExplanation != null) {
                return Explanation.match(score, description, subQueryExplanation);
            }
            return Explanation.match(score, description);
        }
    }

    private static final Map<String, String> DEPRECATIONS = Map.of(
            "doc",
            "Accessing variable [doc] via [params.doc] from within a score script "
                    + "is deprecated in favor of directly accessing [doc].",
            "_doc", "Accessing variable [doc] via [params._doc] from within a score script "
                    + "is deprecated in favor of directly accessing [doc].");

    public static final String[] PARAMETERS = new String[]{ "explanation" };

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** A leaf lookup for the bound segment this script will operate on. */
    private final LeafSearchLookup leafLookup;

    private DoubleSupplier scoreSupplier = () -> 0.0;

    private final int docBase;
    private int docId;
    private int shardId = -1;
    private String indexName = null;
    private Version indexVersion = null;

    public ScoreScript(Map<String, Object> params, SearchLookup lookup, LeafReaderContext leafContext) {
        // null check needed b/c of expression engine subclass
        if (lookup == null) {
            assert params == null;
            assert leafContext == null;
            this.params = null;
            this.leafLookup = null;
            this.docBase = 0;
        } else {
            this.leafLookup = lookup.getLeafSearchLookup(leafContext);
            params = new HashMap<>(params);
            params.putAll(leafLookup.asMap());
            this.params = new DeprecationMap(params, DEPRECATIONS, "score-script");
            this.docBase = leafContext.docBase;
        }
    }

    public abstract double execute(ExplanationHolder explanation);

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** The doc lookup for the Lucene segment this script was created for. */
    public Map<String, ScriptDocValues<?>> getDoc() {
        return leafLookup.doc();
    }

    /** Set the current document to run the script on next. */
    public void setDocument(int docid) {
        this.docId = docid;
        leafLookup.setDocument(docid);
    }

    public void setScorer(Scorable scorer) {
        this.scoreSupplier = () -> {
            try {
                return scorer.score();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    /**
     * Accessed as _score in the painless script
     * @return the score of the inner query
     */
    public double get_score() {
        return scoreSupplier.getAsDouble();
    }


    /**
     * Starting a name with underscore, so that the user cannot access this function directly through a script
     * It is only used within predefined painless functions.
     * @return the internal document ID
     */
    public int _getDocId() {
        return docId;
    }

    /**
     * Starting a name with underscore, so that the user cannot access this function directly through a script
     * It is only used within predefined painless functions.
     * @return the internal document ID with the base
     */
    public int _getDocBaseId() {
        return docBase + docId;
    }

    /**
     *  Starting a name with underscore, so that the user cannot access this function directly through a script
     *  It is only used within predefined painless functions.
     * @return shard id or throws an exception if shard is not set up for this script instance
     */
    public int _getShardId() {
        if (shardId > -1) {
            return shardId;
        } else {
            throw new IllegalArgumentException("shard id can not be looked up!");
        }
    }

    /**
     *  Starting a name with underscore, so that the user cannot access this function directly through a script
     *  It is only used within predefined painless functions.
     * @return index name or throws an exception if the index name is not set up for this script instance
     */
    public String _getIndex() {
        if (indexName != null) {
            return indexName;
        } else {
            throw new IllegalArgumentException("index name can not be looked up!");
        }
    }

    /**
     *  Starting a name with underscore, so that the user cannot access this function directly through a script
     *  It is only used within predefined painless functions.
     * @return index version or throws an exception if the index version is not set up for this script instance
     */
    public Version _getIndexVersion() {
        if (indexVersion != null) {
            return indexVersion;
        } else {
            throw new IllegalArgumentException("index version can not be looked up!");
        }
    }

    /**
     *  Starting a name with underscore, so that the user cannot access this function directly through a script
     */
    public void _setShard(int shardId) {
        this.shardId = shardId;
    }

    /**
     *  Starting a name with underscore, so that the user cannot access this function directly through a script
     */
    public void _setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /**
     *  Starting a name with underscore, so that the user cannot access this function directly through a script
     */
    public void _setIndexVersion(Version indexVersion) {
        this.indexVersion = indexVersion;
    }


    /** A factory to construct {@link ScoreScript} instances. */
    public interface LeafFactory {

        /**
         * Return {@code true} if the script needs {@code _score} calculated, or {@code false} otherwise.
         */
        boolean needs_score();

        ScoreScript newInstance(LeafReaderContext ctx) throws IOException;
    }

    /** A factory to construct stateful {@link ScoreScript} factories for a specific index. */
    public interface Factory extends ScriptFactory {

        ScoreScript.LeafFactory newFactory(Map<String, Object> params, SearchLookup lookup);

    }

    public static final ScriptContext<ScoreScript.Factory> CONTEXT = new ScriptContext<>("score", ScoreScript.Factory.class);
}
