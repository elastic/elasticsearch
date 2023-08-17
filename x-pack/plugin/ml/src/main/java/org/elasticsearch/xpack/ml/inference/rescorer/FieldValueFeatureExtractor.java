/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.rescorer;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class FieldValueFeatureExtractor implements FeatureExtractor {

    record FieldValueFetcher(String fieldName, ValueFetcher valueFetcher) {}

    private LeafReaderContext segmentContext;
    private final List<String> documentFieldNames;
    private final List<FieldValueFetcher> valueFetcherList;
    private final SearchLookup sourceLookup;

    FieldValueFeatureExtractor(List<String> documentFieldNames, SearchExecutionContext executionContext) {
        this.documentFieldNames = documentFieldNames;
        this.valueFetcherList = documentFieldNames.stream().map(s -> {
            MappedFieldType mappedFieldType = executionContext.getFieldType(s);
            if (mappedFieldType != null) {
                return new FieldValueFetcher(s, mappedFieldType.valueFetcher(executionContext, null));
            }
            return null;
        }).filter(Objects::nonNull).toList();
        this.sourceLookup = executionContext.lookup();
    }

    @Override
    public void setNextReader(LeafReaderContext segmentContext) {
        this.segmentContext = segmentContext;
        for (FieldValueFetcher vf : valueFetcherList) {
            vf.valueFetcher().setNextReader(segmentContext);
        }
    }

    @Override
    public void addFeatures(Map<String, Object> featureMap, int docId) throws IOException {
        Source source = sourceLookup.getSource(this.segmentContext, docId);
        for (FieldValueFetcher vf : this.valueFetcherList) {
            featureMap.put(vf.fieldName(), vf.valueFetcher().fetchValues(source, docId, new ArrayList<>()).get(0));
        }
    }

    @Override
    public List<String> featureNames() {
        return documentFieldNames;
    }
}
