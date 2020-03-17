/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The fields to extract for a datafeed that requires a time field
 */
public class TimeBasedExtractedFields extends ExtractedFields {

    private final ExtractedField timeField;

    public TimeBasedExtractedFields(ExtractedField timeField, List<ExtractedField> allFields) {
        super(allFields, Collections.emptyMap());
        if (!allFields.contains(timeField)) {
            throw new IllegalArgumentException("timeField should also be contained in allFields");
        }
        this.timeField = Objects.requireNonNull(timeField);
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

    public static TimeBasedExtractedFields build(Job job, DatafeedConfig datafeed, FieldCapabilitiesResponse fieldsCapabilities) {
        Set<String> scriptFields = datafeed.getScriptFields().stream().map(sf -> sf.fieldName()).collect(Collectors.toSet());
        ExtractionMethodDetector extractionMethodDetector = new ExtractionMethodDetector(scriptFields, fieldsCapabilities);
        String timeField = job.getDataDescription().getTimeField();
        if (scriptFields.contains(timeField) == false && extractionMethodDetector.isAggregatable(timeField) == false) {
            throw new IllegalArgumentException("cannot retrieve time field [" + timeField + "] because it is not aggregatable");
        }
        ExtractedField timeExtractedField = extractedTimeField(timeField, scriptFields);
        List<String> remainingFields = job.allInputFields().stream().filter(f -> !f.equals(timeField)).collect(Collectors.toList());
        List<ExtractedField> allExtractedFields = new ArrayList<>(remainingFields.size() + 1);
        allExtractedFields.add(timeExtractedField);
        remainingFields.forEach(field -> allExtractedFields.add(extractionMethodDetector.detect(field)));

        return new TimeBasedExtractedFields(timeExtractedField, allExtractedFields);
    }

    private static ExtractedField extractedTimeField(String timeField, Set<String> scriptFields) {
        ExtractedField.Method method = scriptFields.contains(timeField) ? ExtractedField.Method.SCRIPT_FIELD
            : ExtractedField.Method.DOC_VALUE;
        return ExtractedFields.newTimeField(timeField, method);
    }
}
