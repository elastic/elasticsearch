/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script.examples.RerankWithDotProductSimilarity

import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.util.VectorUtil
import org.elasticsearch.search.rank.script.ScriptRankDoc


def output = [];
float[] queryVector = new float[params.queryVector.size()];
for (int i = 0; i < queryVector.length; ++i) {
    queryVector[i] = (float) params.queryVector[i];
}
queryVector = VectorUtil.l2normalize(queryVector);

for (ScriptRankDoc scriptRankDoc : inputs[0]) {
    def inputVector = scriptRankDoc.fields()[\"v\"];
    float[] docVector = new float[inputVector.size()];
    for (int i = 0; i < queryVector.length; ++i) {
        docVector[i] = (float) inputVector[i];
    }
    docVector = VectorUtil.l2normalize(docVector);
    float newScore = VectorUtil.dotProduct(queryVector, docVector);
            output.add(new ScoreDoc(
                    scriptRankDoc.scoreDoc().doc,
                    newScore,
                    scriptRankDoc.scoreDoc().shardIndex
            ));
}
output.sort((ScoreDoc sd1, ScoreDoc sd2) -> { return sd1.score < sd2.score ? 1 : -1; });
return output;