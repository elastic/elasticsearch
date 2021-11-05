/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class AnalysisFieldInfo implements DataFrameAnalysis.FieldInfo {

    private final ExtractedFields extractedFields;

    AnalysisFieldInfo(ExtractedFields extractedFields) {
        this.extractedFields = Objects.requireNonNull(extractedFields);
    }

    @Override
    public Set<String> getTypes(String field) {
        Optional<ExtractedField> extractedField = extractedFields.getAllFields().stream().filter(f -> f.getName().equals(field)).findAny();
        return extractedField.isPresent() ? extractedField.get().getTypes() : null;
    }

    @Override
    public Long getCardinality(String field) {
        return extractedFields.getCardinalitiesForFieldsWithConstraints().get(field);
    }
}
