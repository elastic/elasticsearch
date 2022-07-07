/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * The fields the data[feed|frame] has to extract
 */
public class ExtractedFields {

    private final List<ExtractedField> allFields;
    private final List<ExtractedField> docValueFields;
    private final List<ProcessedField> processedFields;
    private final String[] sourceFields;
    private final Map<String, Long> cardinalitiesForFieldsWithConstraints;

    public ExtractedFields(
        List<ExtractedField> allFields,
        List<ProcessedField> processedFields,
        Map<String, Long> cardinalitiesForFieldsWithConstraints
    ) {
        this.allFields = new ArrayList<>(allFields);
        this.docValueFields = filterFields(ExtractedField.Method.DOC_VALUE, allFields);
        this.sourceFields = filterFields(ExtractedField.Method.SOURCE, allFields).stream()
            .map(ExtractedField::getSearchField)
            .toArray(String[]::new);
        this.cardinalitiesForFieldsWithConstraints = Collections.unmodifiableMap(cardinalitiesForFieldsWithConstraints);
        this.processedFields = processedFields == null ? Collections.emptyList() : processedFields;
    }

    public List<ProcessedField> getProcessedFields() {
        return processedFields;
    }

    public List<ExtractedField> getAllFields() {
        return allFields;
    }

    public Set<String> getProcessedFieldInputs() {
        return processedFields.stream().map(ProcessedField::getInputFieldNames).flatMap(List::stream).collect(Collectors.toSet());
    }

    public String[] getSourceFields() {
        return sourceFields;
    }

    public List<ExtractedField> getDocValueFields() {
        return docValueFields;
    }

    public Map<String, Long> getCardinalitiesForFieldsWithConstraints() {
        return cardinalitiesForFieldsWithConstraints;
    }

    public String[] extractOrganicFeatureNames() {
        Set<String> processedFieldInputs = getProcessedFieldInputs();
        return allFields.stream()
            .map(ExtractedField::getName)
            .filter(f -> processedFieldInputs.contains(f) == false)
            .toArray(String[]::new);
    }

    public String[] extractProcessedFeatureNames() {
        return processedFields.stream().map(ProcessedField::getOutputFieldNames).flatMap(List::stream).toArray(String[]::new);
    }

    private static List<ExtractedField> filterFields(ExtractedField.Method method, List<ExtractedField> fields) {
        return fields.stream().filter(field -> field.getMethod() == method).collect(Collectors.toList());
    }

    public static ExtractedFields build(
        Set<String> allFields,
        Set<String> scriptFields,
        Set<String> searchRuntimeFields,
        FieldCapabilitiesResponse fieldsCapabilities,
        Map<String, Long> cardinalitiesForFieldsWithConstraints,
        List<ProcessedField> processedFields
    ) {
        ExtractionMethodDetector extractionMethodDetector = new ExtractionMethodDetector(
            scriptFields,
            fieldsCapabilities,
            searchRuntimeFields
        );
        return new ExtractedFields(
            allFields.stream().map(extractionMethodDetector::detect).collect(Collectors.toList()),
            processedFields,
            cardinalitiesForFieldsWithConstraints
        );
    }

    public static ExtractedFields build(
        Set<String> allFields,
        Set<String> scriptFields,
        FieldCapabilitiesResponse fieldsCapabilities,
        Map<String, Long> cardinalitiesForFieldsWithConstraints,
        List<ProcessedField> processedFields
    ) {
        return build(
            allFields,
            scriptFields,
            Collections.emptySet(),
            fieldsCapabilities,
            cardinalitiesForFieldsWithConstraints,
            processedFields
        );
    }

    public static TimeField newTimeField(String name, ExtractedField.Method method) {
        return new TimeField(name, method);
    }

    public static ExtractedField applyBooleanMapping(ExtractedField field) {
        return new BooleanMapper<>(field, 1, 0);
    }

    public static class ExtractionMethodDetector {

        private final Set<String> scriptFields;
        private final Set<String> searchRuntimeFields;
        private final FieldCapabilitiesResponse fieldsCapabilities;

        public ExtractionMethodDetector(
            Set<String> scriptFields,
            FieldCapabilitiesResponse fieldsCapabilities,
            Set<String> searchRuntimeFields
        ) {
            this.scriptFields = scriptFields;
            this.fieldsCapabilities = fieldsCapabilities;
            this.searchRuntimeFields = searchRuntimeFields;
        }

        public ExtractedField detect(String field) {
            if (scriptFields.contains(field)) {
                return new ScriptField(field);
            }
            if (searchRuntimeFields.contains(field)) {
                return new DocValueField(field, Collections.emptySet());
            }
            ExtractedField extractedField = detectFieldFromFieldCaps(field);
            String parentField = MlStrings.getParentField(field);
            if (isMultiField(field, parentField)) {
                if (isAggregatable(field)) {
                    return new MultiField(parentField, extractedField);
                } else {
                    ExtractedField parentExtractionField = detectFieldFromFieldCaps(parentField);
                    return new MultiField(field, parentField, parentField, parentExtractionField);
                }
            }
            return extractedField;
        }

        private ExtractedField detectFieldFromFieldCaps(String field) {
            if (isFieldOfTypes(field, TimeField.TYPES) && isAggregatable(field)) {
                return new TimeField(field, ExtractedField.Method.DOC_VALUE);
            }
            if (isFieldOfType(field, GeoPointField.TYPE)) {
                if (isAggregatable(field) == false) {
                    throw new IllegalArgumentException("cannot use [geo_point] field with disabled doc values");
                }
                return new GeoPointField(field);
            }
            if (isFieldOfType(field, GeoShapeField.TYPE)) {
                return new GeoShapeField(field);
            }
            Set<String> types = getTypes(field);
            return isAggregatable(field) ? new DocValueField(field, types) : new SourceField(field, types);
        }

        private Set<String> getTypes(String field) {
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            return fieldCaps == null ? Collections.emptySet() : fieldCaps.keySet();
        }

        public boolean isAggregatable(String field) {
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            if (fieldCaps == null || fieldCaps.isEmpty()) {
                throw new IllegalArgumentException("cannot retrieve field [" + field + "] because it has no mappings");
            }
            for (FieldCapabilities capsPerIndex : fieldCaps.values()) {
                if (capsPerIndex.isAggregatable() == false) {
                    return false;
                }
            }
            return true;
        }

        private boolean isFieldOfType(String field, String type) {
            return isFieldOfTypes(field, Collections.singleton(type));
        }

        private boolean isFieldOfTypes(String field, Set<String> types) {
            assert types.isEmpty() == false;
            Map<String, FieldCapabilities> fieldCaps = fieldsCapabilities.getField(field);
            if (fieldCaps != null && fieldCaps.isEmpty() == false) {
                return types.containsAll(fieldCaps.keySet());
            }
            return false;
        }

        private boolean isMultiField(String field, String parent) {
            if (Objects.equals(field, parent)) {
                return false;
            }
            Map<String, FieldCapabilities> parentFieldCaps = fieldsCapabilities.getField(parent);
            if (parentFieldCaps == null || (parentFieldCaps.size() == 1 && isNestedOrObject(parentFieldCaps))) {
                // We check if the parent is an object or nested field. If so, it's not a multi field.
                return false;
            }
            return true;
        }

        private static boolean isNestedOrObject(Map<String, FieldCapabilities> fieldCaps) {
            return fieldCaps.containsKey("object") || fieldCaps.containsKey("nested");
        }
    }

    /**
     * Makes boolean fields behave as a field of different type.
     */
    private static final class BooleanMapper<T> extends AbstractField {

        private static final Set<String> TYPES = Collections.singleton(BooleanFieldMapper.CONTENT_TYPE);

        private final ExtractedField field;
        private final T trueValue;
        private final T falseValue;

        BooleanMapper(ExtractedField field, T trueValue, T falseValue) {
            super(field.getName(), TYPES);
            this.field = field;
            if (field.getTypes().contains(BooleanFieldMapper.CONTENT_TYPE) == false) {
                throw new IllegalArgumentException("cannot apply boolean mapping to field [" + field.getName() + "]");
            }
            this.trueValue = trueValue;
            this.falseValue = falseValue;
        }

        @Override
        public Method getMethod() {
            return field.getMethod();
        }

        @Override
        public Object[] value(SearchHit hit) {
            Object[] value = field.value(hit);
            if (value != null) {
                return Arrays.stream(value).map(v -> {
                    boolean asBoolean;
                    if (v instanceof Boolean) {
                        asBoolean = (boolean) v;
                    } else {
                        asBoolean = Booleans.parseBoolean(v.toString());
                    }
                    return asBoolean ? trueValue : falseValue;
                }).toArray();
            }
            return new Object[0];
        }

        @Override
        public boolean supportsFromSource() {
            return field.supportsFromSource();
        }

        @Override
        public ExtractedField newFromSource() {
            return field.newFromSource();
        }

        @Override
        public boolean isMultiField() {
            return field.isMultiField();
        }

        @Override
        public String getParentField() {
            return field.getParentField();
        }

        @Override
        public String getDocValueFormat() {
            return field.getDocValueFormat();
        }

    }
}
