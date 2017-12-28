/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.MlStrings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The fields the datafeed has to extract
 */
class ExtractedFields {

    private static final String TEXT = "text";

    private final ExtractedField timeField;
    private final List<ExtractedField> allFields;
    private final String[] docValueFields;
    private final String[] sourceFields;

    ExtractedFields(ExtractedField timeField, List<ExtractedField> allFields) {
        if (!allFields.contains(timeField)) {
            throw new IllegalArgumentException("timeField should also be contained in allFields");
        }
        this.timeField = Objects.requireNonNull(timeField);
        this.allFields = Collections.unmodifiableList(allFields);
        this.docValueFields = filterFields(ExtractedField.ExtractionMethod.DOC_VALUE, allFields);
        this.sourceFields = filterFields(ExtractedField.ExtractionMethod.SOURCE, allFields);
    }

    public List<ExtractedField> getAllFields() {
        return allFields;
    }

    public String[] getSourceFields() {
        return sourceFields;
    }

    public String[] getDocValueFields() {
        return docValueFields;
    }

    private static String[] filterFields(ExtractedField.ExtractionMethod method, List<ExtractedField> fields) {
        List<String> result = new ArrayList<>();
        for (ExtractedField field : fields) {
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
            throw new RuntimeException("Time field [" + timeField.getAlias() + "] expected a single value; actual was: "
                    + Arrays.toString(value));
        }
        if (value[0] instanceof Long) {
            return (Long) value[0];
        }
        throw new RuntimeException("Time field [" + timeField.getAlias() + "] expected a long value; actual was: " + value[0]);
    }

    public static ExtractedFields build(Job job, DatafeedConfig datafeed, FieldCapabilitiesResponse fieldsCapabilities) {
        Set<String> scriptFields = datafeed.getScriptFields().stream().map(sf -> sf.fieldName()).collect(Collectors.toSet());
        ExtractionMethodDetector extractionMethodDetector = new ExtractionMethodDetector(datafeed.getId(), scriptFields,
                fieldsCapabilities);
        String timeField = job.getDataDescription().getTimeField();
        if (scriptFields.contains(timeField) == false && extractionMethodDetector.isAggregatable(timeField) == false) {
            throw ExceptionsHelper.badRequestException("datafeed [" + datafeed.getId() + "] cannot retrieve time field [" + timeField
                    + "] because it is not aggregatable");
        }
        ExtractedField timeExtractedField = ExtractedField.newTimeField(timeField, scriptFields.contains(timeField) ?
                ExtractedField.ExtractionMethod.SCRIPT_FIELD : ExtractedField.ExtractionMethod.DOC_VALUE);
        List<String> remainingFields = job.allFields().stream().filter(
                f -> !(f.equals(timeField) || f.equals(AnalysisConfig.ML_CATEGORY_FIELD))).collect(Collectors.toList());
        List<ExtractedField> allExtractedFields = new ArrayList<>(remainingFields.size() + 1);
        allExtractedFields.add(timeExtractedField);
        remainingFields.stream().forEach(field -> allExtractedFields.add(extractionMethodDetector.detect(field)));
        return new ExtractedFields(timeExtractedField, allExtractedFields);
    }

    private static class ExtractionMethodDetector {

        private final String datafeedId;
        private final Set<String> scriptFields;
        private final FieldCapabilitiesResponse fieldsCapabilities;

        private ExtractionMethodDetector(String datafeedId, Set<String> scriptFields, FieldCapabilitiesResponse fieldsCapabilities) {
            this.datafeedId = datafeedId;
            this.scriptFields = scriptFields;
            this.fieldsCapabilities = fieldsCapabilities;
        }

        private ExtractedField detect(String field) {
            String internalField = field;
            ExtractedField.ExtractionMethod method = ExtractedField.ExtractionMethod.SOURCE;
            if (scriptFields.contains(field)) {
                method = ExtractedField.ExtractionMethod.SCRIPT_FIELD;
            } else if (isAggregatable(field)) {
                method = ExtractedField.ExtractionMethod.DOC_VALUE;
            } else if (isText(field)) {
                String parentField = MlStrings.getParentField(field);
                // ThrottlerField is text so check if it is a multi-field
                if (Objects.equals(parentField, field) == false && fieldsCapabilities.getField(parentField) != null) {
                    // ThrottlerField is a multi-field which means it won't be available in source. Let's take the parent instead.
                    internalField = parentField;
                    method = isAggregatable(parentField) ? ExtractedField.ExtractionMethod.DOC_VALUE
                            : ExtractedField.ExtractionMethod.SOURCE;
                }
            }
            return ExtractedField.newField(field, internalField, method);
        }

        private boolean isAggregatable(String field) {
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            if (fieldCaps == null || fieldCaps.isEmpty()) {
                throw ExceptionsHelper.badRequestException("datafeed [" + datafeedId + "] cannot retrieve field [" + field
                        + "] because it has no mappings");
            }
            for (FieldCapabilities capsPerIndex : fieldCaps.values()) {
                if (!capsPerIndex.isAggregatable()) {
                    return false;
                }
            }
            return true;
        }

        private boolean isText(String field) {
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            if (fieldCaps != null && fieldCaps.size() == 1) {
                return fieldCaps.containsKey(TEXT);
            }
            return false;
        }
    }
}
