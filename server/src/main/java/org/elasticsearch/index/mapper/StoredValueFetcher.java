/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Value fetcher that loads from stored values.
 */
public class StoredValueFetcher implements ValueFetcher {

    private final SearchLookup lookup;
    private LeafSearchLookup leafSearchLookup;
    private final String fieldname;
    private final StoredFieldsSpec storedFieldsSpec;

    public StoredValueFetcher(SearchLookup lookup, String fieldname) {
        this.lookup = lookup;
        this.fieldname = fieldname;
        this.storedFieldsSpec = new StoredFieldsSpec(false, false, Set.of(fieldname));
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        this.leafSearchLookup = lookup.getLeafSearchLookup(context);
    }

    @Override
    public List<Object> fetchValues(Source source, int doc, List<Object> ignoredValues) throws IOException {
        leafSearchLookup.setDocument(doc);
        List<Object> values = leafSearchLookup.fields().get(fieldname).getValues();
        if (values == null) {
            return values;
        } else {
            return parseStoredValues(List.copyOf(values));
        }
    }

    /**
     * Given the values stored in lucene, parse it into a standard format.
     */
    protected List<Object> parseStoredValues(List<Object> values) {
        return values;
    }

    @Override
    public StoredFieldsSpec storedFieldsSpec() {
        return storedFieldsSpec;
    }
}
