/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rerank;

import org.elasticsearch.action.search.SearchPhaseController.SortedTopDocs;

import java.util.List;

public interface Reranker {

    default int windowSize() {
        return 0;
    }

    default int size() {
        return 0;
    }

    SortedTopDocs rerank(List<SortedTopDocs> sortedTopDocs);
}
