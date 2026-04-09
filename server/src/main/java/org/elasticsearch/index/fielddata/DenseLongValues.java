/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import org.apache.lucene.search.LongValues;

import java.io.IOException;

/**
 * LongValues implementation that is guaranteed to have a value
 * for every document in a reader
 */
public abstract class DenseLongValues extends LongValues {

    @Override
    public final boolean advanceExact(int doc) throws IOException {
        doAdvanceExact(doc);
        return true;
    }

    protected abstract void doAdvanceExact(int doc) throws IOException;
}
