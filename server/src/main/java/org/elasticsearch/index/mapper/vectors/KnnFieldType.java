/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitSetProducer;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.vectors.VectorData;

public interface KnnFieldType {

    Query createKnnQuery(
        byte[] queryVector,
        int numCands,
        Query filter,
        Float similarityThreshold,
        BitSetProducer parentFilter
    );

    Query createKnnQuery(
        VectorData queryVector,
        int numCands,
        Query filter,
        Float similarityThreshold,
        BitSetProducer parentFilter,
        SearchExecutionContext context);

    Query createExactKnnQuery(VectorData queryVector);
}
