/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scriptrank;

import org.apache.lucene.search.ScoreDoc;

import java.util.Map;

/**
 * The context object for each doc passed into the ScriptRankQuery's script. Each retriever in
 * the original query has a corresponding list of ScriptRankDoc outputs which are passed into
 * to the script.
 */
public final class ScriptRankDoc {
    private final ScoreDoc scoreDoc;
    private final Map<String, Object> fields;
    private final float[] queryScores;

    private Object userContext;

    /**
     * @param scoreDoc    A ScoreDoc containing the doc's (retriever) score, doc id, and shard id.
     * @param fields      A Map of field names to values, as per those requested in the "fields"
     *                    parameter of the script rank query.
     * @param queryScores The scores (in order) for each query specified in the "queries" parameter
     *                    of the script rank query.
     */
    public ScriptRankDoc(ScoreDoc scoreDoc, Map<String, Object> fields, float[] queryScores) {
        this.scoreDoc = scoreDoc;
        this.fields = fields;
        this.queryScores = queryScores;
    }

    /**
     * @return The original score, as assigned by the retriever.
     */
    public float getDocScore() {
        return scoreDoc.score;
    }

    /**
     * Use to access a field, as specified for retrieval by the "fields" param
     * of the request.
     *
     * @param field The name of the field.
     * @return The field value as contained in the _source. Returns null if
     * the given document does not contain this field.
     */
    public Object getField(String field) {
        return fields.get(field);
    }


    /**
     * Returns the score of the nth query as specified by the "query" param of
     * the request.
     *
     * @param queryIndex The index of the query, starting at 0.
     * @return The query's score for this document.
     */
    public float getQueryScore(int queryIndex) throws IndexOutOfBoundsException {
        return queryScores[queryIndex];
    }

    /**
     * TODO update documentation
     * A convenience method for stashing any context for later use by the
     * script. Discarded after script execution.
     * <p>
     * Examples include script-generated scores/ranks, which can then be
     * accessed at a later time by the script for sorting/ordering results
     * prior to returning.
     */
    public Object getUserContext() {
        return userContext;
    }

    /**
     * TODO update documentation
     * A convenience method for stashing any context for later use by the
     * script. Discarded after script execution.
     * <p>
     * Examples include script-generated scores/ranks, which can then be
     * accessed at a later time by the script for sorting/ordering results
     * prior to returning.
     *
     * @param userCtx
     */
    public void setUserContext(Object userCtx) {
        this.userContext = userCtx;
    }
}
