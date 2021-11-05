/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.search.Explanation;

import java.io.IOException;

/**
 * To be implemented by {@link ScoreScript} which can provided an {@link Explanation} of the score
 * This is currently not used inside elasticsearch but it is used, see for example here:
 * https://github.com/elastic/elasticsearch/issues/8561
 */
public interface ExplainableScoreScript {

    /**
     * Build the explanation of the current document being scored
     * The script score needs the Explanation of the sub query score because it might use _score and
     * want to explain how that was computed.
     *
     * @param subQueryScore the Explanation for _score
     */
    Explanation explain(Explanation subQueryScore) throws IOException;

}
