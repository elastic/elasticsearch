/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.search.SearchHit;
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

    private final List<ExtractedField> allFields;
    private final List<ExtractedField> docValueFields;
    private final String[] sourceFields;
    private final Map<String, Long> cardinalitiesForFieldsWithConstraints;

    public ExtractedFields(List<ExtractedField> allFields, Map<String, Long> cardinalitiesForFieldsWithConstraints) {
        this.allFields = Collections.unmodifiableList(allFields);
        this.docValueFields = filterFields(ExtractedField.Method.DOC_VALUE, allFields);
        this.sourceFields = filterFields(ExtractedField.Method.SOURCE, allFields).stream().map(ExtractedField::getSearchField)
            .toArray(String[]::new);
        this.cardinalitiesForFieldsWithConstraints = Collections.unmodifiableMap(cardinalitiesForFieldsWithConstraints);
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

    public Map<String, Long> getCardinalitiesForFieldsWithConstraints() {
        return cardinalitiesForFieldsWithConstraints;
    }

    private static List<ExtractedField> filterFields(ExtractedField.Method method, List<ExtractedField> fields) {
        return fields.stream().filter(field -> field.getMethod() == method).collect(Collectors.toList());
    }

    public static ExtractedFields build(Collection<String> allFields, Set<String> scriptFields,
                                        FieldCapabilitiesResponse fieldsCapabilities,
                                        Map<String, Long> cardinalitiesForFieldsWithConstraints) {
        ExtractionMethodDetector extractionMethodDetector = new ExtractionMethodDetector(scriptFields, fieldsCapabilities);
        return new ExtractedFields(allFields.stream().map(field -> extractionMethodDetector.detect(field)).collect(Collectors.toList()),
            cardinalitiesForFieldsWithConstraints);
    }

    public static TimeField newTimeField(String name, ExtractedField.Method method) {
        return new TimeField(name, method);
    }

    public static ExtractedField applyBooleanMapping(ExtractedField field) {
        return new BooleanMapper<>(field, 1, 0);
    }

    public static class ExtractionMethodDetector {

        private final Set<String> scriptFields;
        private final FieldCapabilitiesResponse fieldsCapabilities;

        public ExtractionMethodDetector(Set<String> scriptFields, FieldCapabilitiesResponse fieldsCapabilities) {
            this.scriptFields = scriptFields;
            this.fieldsCapabilities = fieldsCapabilities;
        }

        public ExtractedField detect(String field) {
            if (scriptFields.contains(field)) {
                return new ScriptField(field);
            }
            ExtractedField extractedField = detectNonScriptField(field);
            String parentField = MlStrings.getParentField(field);
            if (isMultiField(field, parentField)) {
                if (isAggregatable(field)) {
                    return new MultiField(parentField, extractedField);
                } else {
                    ExtractedField parentExtractionField = detectNonScriptField(parentField);
                    return new MultiField(field, parentField, parentField, parentExtractionField);
                }
            }
            return extractedField;
        }

        private ExtractedField detectNonScriptField(String field) {
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
                if (!capsPerIndex.isAggregatable()) {
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
            if (parentFieldCaps == null || (parentFieldCaps.size() == 1 && parentFieldCaps.containsKey("object"))) {
                // We check if the parent is an object which is indicated by field caps containing an "object" entry.
                // If an object, it's not a multi field
                return false;
            }
            return true;
        }
    }

    /**
     * Makes boolean fields behave as a field of different type.
     */
    private static final class BooleanMapper<T> extends DocValueField {

        private static final Set<String> TYPES = Collections.singleton(BooleanFieldMapper.CONTENT_TYPE);

        private final T trueValue;
        private final T falseValue;

        BooleanMapper(ExtractedField field, T trueValue, T falseValue) {
            super(field.getName(), TYPES);
            if (field.getMethod() != Method.DOC_VALUE || field.getTypes().contains(BooleanFieldMapper.CONTENT_TYPE) == false) {
                throw new IllegalArgumentException("cannot apply boolean mapping to field [" + field.getName() + "]");
            }
            this.trueValue = trueValue;
            this.falseValue = falseValue;
        }

        @Override
        public Object[] value(SearchHit hit) {
            DocumentField keyValue = hit.field(getName());
            if (keyValue != null) {
                return keyValue.getValues().stream().map(v -> Boolean.TRUE.equals(v) ? trueValue : falseValue).toArray();
            }
            return new Object[0];
        }

        @Override
        public boolean supportsFromSource() {
            return false;
        }

        @Override
        public ExtractedField newFromSource() {
            throw new UnsupportedOperationException();
        }
    }
}
