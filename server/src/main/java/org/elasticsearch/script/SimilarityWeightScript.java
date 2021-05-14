/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.index.similarity.ScriptedSimilarity;

/** A script that is used to compute scoring factors that are the same for all documents. */
public abstract class SimilarityWeightScript  {

    /** Compute the weight.
     * @param query  scoring factors that come from the query
     * @param field  field-level statistics
     * @param term   term-level statistics
     */
    public abstract double execute(ScriptedSimilarity.Query query, ScriptedSimilarity.Field field,
            ScriptedSimilarity.Term term);

    public interface Factory extends ScriptFactory {
        SimilarityWeightScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] {"query", "field", "term"};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("similarity_weight", Factory.class);
}
