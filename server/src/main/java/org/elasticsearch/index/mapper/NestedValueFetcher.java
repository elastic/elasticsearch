/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.mapper;

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
import java.util.Set;

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
    public List<Object> fetchValues(SourceLookup lookup, Set<String> ignoreFields) throws IOException {
        List<Object> nestedEntriesToReturn = new ArrayList<>();
        Map<String, Object> filteredSource = new HashMap<>();
        Map<String, Object> stub = createSourceMapStub(filteredSource);
        List<?> nestedValues = XContentMapValues.extractNestedValue(nestedFieldPath, (lookup.loadSourceIfNeeded()));
        if (nestedValues == null) {
            return Collections.emptyList();
        }
        for (Object entry : nestedValues) {
            // add this one entry only to the stub and use this as source lookup
            stub.put(nestedFieldName, entry);
            SourceLookup nestedSourceLookup = new SourceLookup();
            nestedSourceLookup.setSource(filteredSource);

            Map<String, DocumentField> fetchResult = nestedFieldFetcher.fetch(nestedSourceLookup, ignoreFields);

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
}
