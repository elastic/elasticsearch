/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Value fetcher that loads from doc values.
 */
public final class DocValueFetcher implements ValueFetcher {
    private final DocValueFormat format;
    private final IndexFieldData<?> ifd;
    private Leaf leaf;

    public DocValueFetcher(DocValueFormat format, IndexFieldData<?> ifd) {
        this.format = format;
        this.ifd = ifd;
    }

    public void setNextReader(LeafReaderContext context) {
        leaf = ifd.load(context).getLeafValueFetcher(format);
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup) throws IOException {
        if (false == leaf.advanceExact(lookup.docId())) {
            return emptyList();
        }
        List<Object> result = new ArrayList<Object>(leaf.docValueCount());
        for (int i = 0, count = leaf.docValueCount(); i < count; ++i) {
            result.add(leaf.nextValue());
        }
        return result;
    }

    public interface Leaf {
        /**
         * Advance the doc values reader to the provided doc.
         * @return false if there are no values for this document, true otherwise
         */
        boolean advanceExact(int docId) throws IOException;

        /**
         * A count of the number of values at this document.
         */
        int docValueCount() throws IOException;

        /**
         * Load and format the next value.
         */
        Object nextValue() throws IOException;
    }
}
