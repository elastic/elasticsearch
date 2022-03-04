/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.index.similarity.ScriptedSimilarity;

/** A script that is used to build {@link ScriptedSimilarity} instances. */
public abstract class SimilarityScript {

    /** Compute the score.
     * @param weight weight computed by the {@link SimilarityWeightScript} if any, or 1.
     * @param query  scoring factors that come from the query
     * @param field  field-level statistics
     * @param term   term-level statistics
     * @param doc    per-document statistics
     */
    public abstract double execute(
        double weight,
        ScriptedSimilarity.Query query,
        ScriptedSimilarity.Field field,
        ScriptedSimilarity.Term term,
        ScriptedSimilarity.Doc doc
    );

    public interface Factory extends ScriptFactory {
        SimilarityScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] { "weight", "query", "field", "term", "doc" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("similarity", Factory.class);
}
