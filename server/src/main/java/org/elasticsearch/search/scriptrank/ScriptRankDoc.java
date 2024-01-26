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
 *
 * @param scoreDoc    A ScoreDoc containing the doc's (retriever) score, doc id, and shard id.
 * @param fields      A Map of field names to values, as per those requested in the "fields"
 *                    parameter of the script rank query.
 * @param queryScores The scores (in order) for each query specified in the "queries" parameter
 *                    of the script rank query.
 */
public record ScriptRankDoc(ScoreDoc scoreDoc, Map<String, Object> fields, float[] queryScores) {
}
