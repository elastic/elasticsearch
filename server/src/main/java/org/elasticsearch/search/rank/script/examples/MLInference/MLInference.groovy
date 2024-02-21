/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script.examples.MLInference

import org.apache.lucene.search.ScoreDoc
import org.elasticsearch.search.rank.script.ScriptRankDoc



StringBuilder sb = new StringBuilder();
List output = [];
for (ScriptRankDoc scriptRankDoc : inputs[0]) {
    sb.append(scriptRankDoc.queryScores()[0]);
    sb.append(\"|\");
            float newScore = scriptRankDoc.queryScores()[0];
            output.add(new ScoreDoc(scriptRankDoc.scoreDoc().doc, newScore, scriptRankDoc.scoreDoc().shardIndex));
            sb.append(output.get(output.size() - 1));
            sb.append(\"|\");
}
/*if (output != null) throw new IllegalArgumentException(sb.toString());*/
output.sort((ScoreDoc sd1, ScoreDoc sd2) -> {
    return sd1.score < sd2.score ? 1 : -1;
});
return output;
