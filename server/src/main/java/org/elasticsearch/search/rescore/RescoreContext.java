/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rescore;

import org.apache.lucene.search.Query;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Context available to the rescore while it is running. Rescore
 * implementations should extend this with any additional resources that
 * they will need while rescoring.
 */
public class RescoreContext {
    private final int windowSize;
    private final Rescorer rescorer;
    private Set<Integer> rescoredDocs; //doc Ids for which rescoring was applied

    /**
     * Build the context.
     * @param rescorer the rescorer actually performing the rescore.
     */
    public RescoreContext(int windowSize, Rescorer rescorer) {
        this.windowSize = windowSize;
        this.rescorer = rescorer;
    }

    /**
     * The rescorer to actually apply.
     */
    public Rescorer rescorer() {
        return rescorer;
    }

    /**
     * Size of the window to rescore.
     */
    public int getWindowSize() {
        return windowSize;
    }

    public void setRescoredDocs(Set<Integer> docIds) {
        rescoredDocs = docIds;
    }

    public boolean isRescored(int docId) {
        return rescoredDocs != null && rescoredDocs.contains(docId);
    }

    public Set<Integer> getRescoredDocs() {
        return rescoredDocs;
    }

    /**
     * Returns queries associated with the rescorer
     */
    public List<Query> getQueries() {
        return Collections.emptyList();
    }
}
