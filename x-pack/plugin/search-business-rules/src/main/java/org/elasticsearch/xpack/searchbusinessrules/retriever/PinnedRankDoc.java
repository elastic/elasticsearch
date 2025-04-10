/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules.retriever;

import org.elasticsearch.search.rank.RankDoc;

public class PinnedRankDoc extends RankDoc {
    private final boolean isPinned;
    private final String pinnedBy;

    public PinnedRankDoc(int docId, float score, int shardIndex, boolean isPinned, String pinnedBy) {
        super(docId, score, shardIndex);
        this.isPinned = isPinned;
        this.pinnedBy = pinnedBy;
    }

    public boolean isPinned() {
        return isPinned;
    }

    public String getPinnedBy() {
        return pinnedBy;
    }

    @Override
    public String toString() {
        return super.toString() + ", isPinned=" + isPinned + ", pinnedBy=" + pinnedBy;
    }
}
