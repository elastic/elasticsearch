/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.search.TopDocs;

/**
 * Wrapper around a {@link TopDocs} instance and the maximum score.
 */
// TODO: Remove this class when https://github.com/elastic/elasticsearch/issues/32981 is addressed.
public final class TopDocsAndMaxScore {

    public final TopDocs topDocs;
    public float maxScore;

    public TopDocsAndMaxScore(TopDocs topDocs, float maxScore) {
        this.topDocs = topDocs;
        this.maxScore = maxScore;
    }

}
