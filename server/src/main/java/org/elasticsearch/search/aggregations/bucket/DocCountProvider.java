/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.elasticsearch.index.mapper.DocCountFieldMapper;

import java.io.IOException;

/**
 * An implementation of a doc_count provider that reads the value
 * of the _doc_count field in the document. If a document does not have a
 * _doc_count field the implementation will return 1 as the default value.
 *
 * <p>New indices store doc counts as numeric doc values; older indices store them as term
 * frequencies in the inverted index. Both formats are handled transparently.
 */
public final class DocCountProvider {

    public static final int DEFAULT_VALUE = DocCountFieldMapper.DocCountFieldType.DEFAULT_VALUE;

    private NumericDocValues docCountValues;
    private PostingsEnum docCountPostings;

    public int getDocCount(int doc) throws IOException {
        if (docCountValues != null) {
            if (docCountValues.advanceExact(doc)) {
                return (int) docCountValues.longValue();
            }
            return DEFAULT_VALUE;
        }
        if (docCountPostings == null) {
            return DEFAULT_VALUE;
        }
        if (docCountPostings.docID() < doc) {
            docCountPostings.advance(doc);
        }
        if (docCountPostings.docID() == doc) {
            return docCountPostings.freq();
        } else {
            return DEFAULT_VALUE;
        }
    }

    public void setLeafReaderContext(LeafReaderContext ctx) throws IOException {
        docCountValues = DocCountFieldMapper.numericDocValuesLookup(ctx.reader());
        if (docCountValues == null) {
            docCountPostings = DocCountFieldMapper.leafLookup(ctx.reader());
        } else {
            docCountPostings = null;
        }
    }

    public boolean alwaysOne() {
        return docCountValues == null && docCountPostings == null;
    }

    @Override
    public String toString() {
        return "doc counts are "
            + (alwaysOne() ? "always one" : "based on " + (docCountValues != null ? docCountValues : docCountPostings));
    }
}
