/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.fields;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The fields the datafeed has to extract
 */
public class ExtractedFields {

    private static final String TEXT = "text";

    private final List<ExtractedField> allFields;
    private final List<ExtractedField> docValueFields;
    private final String[] sourceFields;

    public ExtractedFields(List<ExtractedField> allFields) {
        this.allFields = Collections.unmodifiableList(allFields);
        this.docValueFields = filterFields(ExtractedField.ExtractionMethod.DOC_VALUE, allFields);
        this.sourceFields = filterFields(ExtractedField.ExtractionMethod.SOURCE, allFields).stream().map(ExtractedField::getName)
            .toArray(String[]::new);
    }

    public List<ExtractedField> getAllFields() {
        return allFields;
    }

    public String[] getSourceFields() {
        return sourceFields;
    }

    public List<ExtractedField> getDocValueFields() {
        return docValueFields;
    }

    private static List<ExtractedField> filterFields(ExtractedField.ExtractionMethod method, List<ExtractedField> fields) {
        return fields.stream().filter(field -> field.getExtractionMethod() == method).collect(Collectors.toList());
    }

    public static ExtractedFields build(Collection<String> allFields, Set<String> scriptFields,
                                        FieldCapabilitiesResponse fieldsCapabilities) {
        ExtractionMethodDetector extractionMethodDetector = new ExtractionMethodDetector(scriptFields, fieldsCapabilities);
        return new ExtractedFields(allFields.stream().map(field -> extractionMethodDetector.detect(field)).collect(Collectors.toList()));
    }

    protected static class ExtractionMethodDetector {

        private final Set<String> scriptFields;
        private final FieldCapabilitiesResponse fieldsCapabilities;

        protected ExtractionMethodDetector(Set<String> scriptFields, FieldCapabilitiesResponse fieldsCapabilities) {
            this.scriptFields = scriptFields;
            this.fieldsCapabilities = fieldsCapabilities;
        }

        protected ExtractedField detect(String field) {
            String internalField = field;
            ExtractedField.ExtractionMethod method = ExtractedField.ExtractionMethod.SOURCE;
            Set<String> types = getTypes(field);
            if (scriptFields.contains(field)) {
                method = ExtractedField.ExtractionMethod.SCRIPT_FIELD;
            } else if (isAggregatable(field)) {
                method = ExtractedField.ExtractionMethod.DOC_VALUE;
                if (isFieldOfType(field, "date")) {
                    return ExtractedField.newTimeField(field, types, method);
                }
            } else if (isFieldOfType(field, TEXT)) {
                String parentField = MlStrings.getParentField(field);
                // Field is text so check if it is a multi-field
                if (Objects.equals(parentField, field) == false && fieldsCapabilities.getField(parentField) != null) {
                    // Field is a multi-field which means it won't be available in source. Let's take the parent instead.
                    internalField = parentField;
                    method = isAggregatable(parentField) ? ExtractedField.ExtractionMethod.DOC_VALUE
                            : ExtractedField.ExtractionMethod.SOURCE;
                }
            }

            if (isFieldOfType(field, "geo_point")) {
                if (method != ExtractedField.ExtractionMethod.DOC_VALUE) {
                    throw new IllegalArgumentException("cannot use [geo_point] field with disabled doc values");
                }
                return ExtractedField.newGeoPointField(field, internalField);
            }
            if (isFieldOfType(field, "geo_shape")) {
                return ExtractedField.newGeoShapeField(field, internalField);
            }

            return ExtractedField.newField(field, internalField, types, method);
        }

        private Set<String> getTypes(String field) {
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            return fieldCaps == null ? Collections.emptySet() : fieldCaps.keySet();
        }

        protected boolean isAggregatable(String field) {
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            if (fieldCaps == null || fieldCaps.isEmpty()) {
                throw new IllegalArgumentException("cannot retrieve field [" + field + "] because it has no mappings");
            }
            for (FieldCapabilities capsPerIndex : fieldCaps.values()) {
                if (!capsPerIndex.isAggregatable()) {
                    return false;
                }
            }
            return true;
        }

        private boolean isFieldOfType(String field, String type) {
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            if (fieldCaps != null && fieldCaps.size() == 1) {
                return fieldCaps.containsKey(type);
            }
            return false;
        }
    }
}
