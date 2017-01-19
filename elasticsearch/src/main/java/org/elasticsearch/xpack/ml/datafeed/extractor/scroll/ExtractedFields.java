/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

class ExtractedFields {

    private final ExtractedField timeField;
    private final List<ExtractedField> allFields;

    public ExtractedFields(ExtractedField timeField, List<ExtractedField> allFields) {
        if (!allFields.contains(timeField)) {
            throw new IllegalArgumentException("timeField should also be contained in allFields");
        }
        this.timeField = Objects.requireNonNull(timeField);
        this.allFields = Collections.unmodifiableList(allFields);
    }

    public List<ExtractedField> getAllFields() {
        return allFields;
    }

    public String[] getSourceFields() {
        return filterFields(ExtractedField.ExtractionMethod.SOURCE);
    }

    public String[] getDocValueFields() {
        return filterFields(ExtractedField.ExtractionMethod.DOC_VALUE);
    }

    private String[] filterFields(ExtractedField.ExtractionMethod method) {
        List<String> result = new ArrayList<>();
        for (ExtractedField field : allFields) {
            if (field.getExtractionMethod() == method) {
                result.add(field.getName());
            }
        }
        return result.toArray(new String[result.size()]);
    }

    public String timeField() {
        return timeField.getName();
    }

    public Long timeFieldValue(SearchHit hit) {
        Object[] value = timeField.value(hit);
        if (value.length != 1) {
            throw new RuntimeException("Time field [" + timeField.getName() + "] expected a single value; actual was: "
                    + Arrays.toString(value));
        }
        if (value[0] instanceof Long) {
            return (Long) value[0];
        }
        throw new RuntimeException("Time field [" + timeField.getName() + "] expected a long value; actual was: " + value[0]);
    }

    public static ExtractedFields build(Job job, DatafeedConfig datafeedConfig) {
        Set<String> scriptFields = datafeedConfig.getScriptFields().stream().map(sf -> sf.fieldName()).collect(Collectors.toSet());
        String timeField = job.getDataDescription().getTimeField();
        ExtractedField timeExtractedField = ExtractedField.newField(timeField, scriptFields.contains(timeField) ?
                ExtractedField.ExtractionMethod.SCRIPT_FIELD : ExtractedField.ExtractionMethod.DOC_VALUE);
        List<String> remainingFields = job.allFields().stream().filter(f -> !f.equals(timeField)).collect(Collectors.toList());
        List<ExtractedField> allExtractedFields = new ArrayList<>(remainingFields.size());
        allExtractedFields.add(timeExtractedField);
        for (String field : remainingFields) {
            ExtractedField.ExtractionMethod method = scriptFields.contains(field) ? ExtractedField.ExtractionMethod.SCRIPT_FIELD :
                    datafeedConfig.isSource() ? ExtractedField.ExtractionMethod.SOURCE : ExtractedField.ExtractionMethod.DOC_VALUE;
            allExtractedFields.add(ExtractedField.newField(field, method));
        }
        return new ExtractedFields(timeExtractedField, allExtractedFields);
    }
}
