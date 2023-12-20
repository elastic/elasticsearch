/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.elasticsearch.index.mapper.DocCountFieldMapper;

import java.io.IOException;

/**
 * An implementation of a doc_count provider that reads the value
 * of the _doc_count field in the document. If a document does not have a
 * _doc_count field the implementation will return 1 as the default value.
 */
public class DocCountProvider {

    public static final int DEFAULT_VALUE = DocCountFieldMapper.DocCountFieldType.DEFAULT_VALUE;

    private PostingsEnum docCountPostings;

    public int getDocCount(int doc) throws IOException {
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
        docCountPostings = DocCountFieldMapper.leafLookup(ctx.reader());
    }

    public boolean alwaysOne() {
        return docCountPostings == null;
    }

    @Override
    public String toString() {
        return "doc counts are " + (alwaysOne() ? "always one" : "based on " + docCountPostings);
    }
}
