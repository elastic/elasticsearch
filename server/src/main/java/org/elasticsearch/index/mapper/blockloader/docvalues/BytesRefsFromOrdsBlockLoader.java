/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader.docvalues;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

/**
 * Loads {@code keyword} style fields that are stored as a lookup table and ordinals.
 */
public class BytesRefsFromOrdsBlockLoader extends AbstractBytesRefsFromOrdsBlockLoader {
    public BytesRefsFromOrdsBlockLoader(String fieldName) {
        super(fieldName);
    }

    @Override
    protected AllReader singletonReader(SortedDocValues docValues) {
        return new Singleton(docValues);
    }

    @Override
    protected AllReader sortedSetReader(SortedSetDocValues docValues) {
        return new SortedSet(docValues);
    }

    @Override
    public boolean supportsOrdinals() {
        return true;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) throws IOException {
        return DocValues.getSortedSet(context.reader(), fieldName);
    }

    @Override
    public String toString() {
        return "BytesRefsFromOrds[" + fieldName + "]";
    }
}
