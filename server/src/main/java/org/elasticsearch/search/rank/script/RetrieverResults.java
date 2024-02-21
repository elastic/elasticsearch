/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.script;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RetrieverResults extends AbstractList<List<ScriptRankDoc>> {
    private final List<List<ScriptRankDoc>> scriptRankDocs;

    public RetrieverResults(List<List<ScriptRankDoc>> scriptRankDocs) {
        List<List<ScriptRankDoc>> results = new ArrayList<>(scriptRankDocs.size());
        for (var retrieverResult : scriptRankDocs) {
            results.add(Collections.unmodifiableList(retrieverResult));
        }
        this.scriptRankDocs = Collections.unmodifiableList(results);
    }

    @Override
    public List<ScriptRankDoc> get(int retrieverIndex) throws IndexOutOfBoundsException {
        return scriptRankDocs.get(retrieverIndex);
    }

    @Override
    public int size() {
        return scriptRankDocs.size();
    }
}
