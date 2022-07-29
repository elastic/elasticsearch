/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.List;

/**
 * Value fetcher that loads from stored values.
 */
public final class StoredValueFetcher implements ValueFetcher {

    private final SearchLookup lookup;
    private LeafSearchLookup leafSearchLookup;
    private final String fieldname;

    public StoredValueFetcher(SearchLookup lookup, String fieldname) {
        this.lookup = lookup;
        this.fieldname = fieldname;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        this.leafSearchLookup = lookup.getLeafSearchLookup(context);
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup, List<Object> ignoredValues) throws IOException {
        leafSearchLookup.setDocument(lookup.docId());
        return leafSearchLookup.fields().get(fieldname).getValues();
    }

}
