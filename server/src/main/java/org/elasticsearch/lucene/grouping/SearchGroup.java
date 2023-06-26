/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.lucene.grouping;

import org.apache.lucene.search.ScoreDoc;

class SearchGroup<T> extends ScoreDoc {
    T groupValue;
    int slot;

    SearchGroup(int doc, int slot, T groupValue) {
        super(doc, Float.NaN);
        this.slot = slot;
        this.groupValue = groupValue;
    }

    @Override
    public String toString() {
        return "slot:" + slot + " " + super.toString();
    }
}
