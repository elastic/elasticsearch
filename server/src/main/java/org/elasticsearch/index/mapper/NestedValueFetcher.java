/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.fetch.subphase.FieldFetcher;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NestedValueFetcher implements ValueFetcher {

    private final String nestedFieldPath;
    private final FieldFetcher nestedFieldFetcher;

    // the name of the nested field without the full path, i.e. in foo.bar.baz it would be baz
    private final String nestedFieldName;
    private final String[] nestedPathParts;

    public NestedValueFetcher(String nestedField, FieldFetcher nestedFieldFetcher) {
        assert nestedField != null && nestedField.isEmpty() == false;
        this.nestedFieldPath = nestedField;
        this.nestedFieldFetcher = nestedFieldFetcher;
        this.nestedPathParts = nestedFieldPath.split("\\.");
        this.nestedFieldName = nestedPathParts[nestedPathParts.length - 1];
    }

    @Override
    public List<Object> fetchValues(SourceLookup lookup, List<Object> includedValues) throws IOException {
        List<Object> nestedEntriesToReturn = new ArrayList<>();
        Map<String, Object> filteredSource = new HashMap<>();
        Map<String, Object> stub = createSourceMapStub(filteredSource);
        List<?> nestedValues = XContentMapValues.extractNestedSources(nestedFieldPath, lookup.source());
        if (nestedValues == null) {
            return Collections.emptyList();
        }
        for (Object entry : nestedValues) {
            // add this one entry only to the stub and use this as source lookup
            stub.put(nestedFieldName, entry);
            SourceLookup nestedSourceLookup = new SourceLookup(new SourceLookup.MapSourceProvider(filteredSource));

            Map<String, DocumentField> fetchResult = nestedFieldFetcher.fetch(nestedSourceLookup);

            Map<String, Object> nestedEntry = new HashMap<>();
            for (DocumentField field : fetchResult.values()) {
                List<Object> fetchValues = field.getValues();
                if (fetchValues.isEmpty() == false) {
                    String keyInNestedMap = field.getName().substring(nestedFieldPath.length() + 1);
                    nestedEntry.put(keyInNestedMap, fetchValues);
                }
            }
            if (nestedEntry.isEmpty() == false) {
                nestedEntriesToReturn.add(nestedEntry);
            }
        }
        return nestedEntriesToReturn;
    }

    // create a filtered source map stub which contains the nested field path
    private Map<String, Object> createSourceMapStub(Map<String, Object> filteredSource) {
        Map<String, Object> next = filteredSource;
        for (int i = 0; i < nestedPathParts.length - 1; i++) {
            String part = nestedPathParts[i];
            Map<String, Object> newMap = new HashMap<>();
            next.put(part, newMap);
            next = newMap;
        }
        return next;
    }

    @Override
    public void setNextReader(LeafReaderContext context) {
        this.nestedFieldFetcher.setNextReader(context);
    }
}
