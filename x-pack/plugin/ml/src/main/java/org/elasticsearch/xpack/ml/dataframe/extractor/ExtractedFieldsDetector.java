/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.FieldCardinalityConstraint;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RequiredField;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Types;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NameResolver;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtractedFieldsDetector {

    private static final Logger LOGGER = LogManager.getLogger(ExtractedFieldsDetector.class);

    /**
     * Fields to ignore. These are mostly internal meta fields.
     */
    private static final List<String> IGNORE_FIELDS = Arrays.asList("_id", "_field_names", "_index", "_parent", "_routing", "_seq_no",
        "_source", "_type", "_uid", "_version", "_feature", "_ignored", "_nested_path", DestinationIndex.ID_COPY,
        "_data_stream_timestamp");

    private final DataFrameAnalyticsConfig config;
    private final int docValueFieldsLimit;
    private final FieldCapabilitiesResponse fieldCapabilitiesResponse;
    private final Map<String, Long> cardinalitiesForFieldsWithConstraints;

    ExtractedFieldsDetector(DataFrameAnalyticsConfig config, int docValueFieldsLimit, FieldCapabilitiesResponse fieldCapabilitiesResponse,
                            Map<String, Long> cardinalitiesForFieldsWithConstraints) {
        this.config = Objects.requireNonNull(config);
        this.docValueFieldsLimit = docValueFieldsLimit;
        this.fieldCapabilitiesResponse = Objects.requireNonNull(fieldCapabilitiesResponse);
        this.cardinalitiesForFieldsWithConstraints = Objects.requireNonNull(cardinalitiesForFieldsWithConstraints);
    }

    public Tuple<ExtractedFields, List<FieldSelection>> detect() {
        TreeSet<FieldSelection> fieldSelection = new TreeSet<>(Comparator.comparing(FieldSelection::getName));
        Set<String> fields = getIncludedFields(fieldSelection);
        checkFieldsHaveCompatibleTypes(fields);
        checkRequiredFields(fields);
        checkFieldsWithCardinalityLimit();
        ExtractedFields extractedFields = detectExtractedFields(fields, fieldSelection);
        addIncludedFields(extractedFields, fieldSelection);

        return Tuple.tuple(extractedFields, Collections.unmodifiableList(new ArrayList<>(fieldSelection)));
    }

    private Set<String> getIncludedFields(Set<FieldSelection> fieldSelection) {
        Set<String> fields = new TreeSet<>(fieldCapabilitiesResponse.get().keySet());
        fields.removeAll(IGNORE_FIELDS);
        removeFieldsUnderResultsField(fields);
        removeObjects(fields);
        applySourceFiltering(fields);
        FetchSourceContext analyzedFields = config.getAnalyzedFields();

        // If the user has not explicitly included fields we'll include all compatible fields
        if (analyzedFields == null || analyzedFields.includes().length == 0) {
            removeFieldsWithIncompatibleTypes(fields, fieldSelection);
        }
        includeAndExcludeFields(fields, fieldSelection);

        return fields;
    }

    private void removeFieldsUnderResultsField(Set<String> fields) {
        String resultsField = config.getDest().getResultsField();
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            if (field.startsWith(resultsField + ".")) {
                fieldsIterator.remove();
            }
        }
        fields.removeIf(field -> field.startsWith(resultsField + "."));
    }

    private void removeObjects(Set<String> fields) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            Set<String> types = getMappingTypes(field);
            if (isObject(types)) {
                fieldsIterator.remove();
            }
        }
    }

    private void applySourceFiltering(Set<String> fields) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            if (config.getSource().isFieldExcluded(field)) {
                fieldsIterator.remove();
            }
        }
    }

    private void addExcludedField(String field, String reason, Set<FieldSelection> fieldSelection) {
        fieldSelection.add(FieldSelection.excluded(field, getMappingTypes(field), reason));
    }

    private Set<String> getMappingTypes(String field) {
        Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(field);
        return fieldCaps == null ? Collections.emptySet() : fieldCaps.keySet();
    }

    private void removeFieldsWithIncompatibleTypes(Set<String> fields, Set<FieldSelection> fieldSelection) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            if (hasCompatibleType(field) == false) {
                addExcludedField(field, "unsupported type; supported types are " + getSupportedTypes(), fieldSelection);
                fieldsIterator.remove();
            }
        }
    }

    private boolean hasCompatibleType(String field) {
        Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(field);
        if (fieldCaps == null) {
            LOGGER.debug("[{}] incompatible field [{}] because it is missing from mappings", config.getId(), field);
            return false;
        }
        Set<String> fieldTypes = fieldCaps.keySet();
        if (Types.numerical().containsAll(fieldTypes)) {
            LOGGER.debug("[{}] field [{}] is compatible as it is numerical", config.getId(), field);
            return true;
        } else if (config.getAnalysis().supportsCategoricalFields() && Types.categorical().containsAll(fieldTypes)) {
            LOGGER.debug("[{}] field [{}] is compatible as it is categorical", config.getId(), field);
            return true;
        } else if (isBoolean(fieldTypes)) {
            LOGGER.debug("[{}] field [{}] is compatible as it is boolean", config.getId(), field);
            return true;
        } else {
            LOGGER.debug("[{}] incompatible field [{}]; types {}; supported {}", config.getId(), field, fieldTypes, getSupportedTypes());
            return false;
        }
    }

    private Set<String> getSupportedTypes() {
        Set<String> supportedTypes = new TreeSet<>(Types.numerical());
        if (config.getAnalysis().supportsCategoricalFields()) {
            supportedTypes.addAll(Types.categorical());
        }
        supportedTypes.add(BooleanFieldMapper.CONTENT_TYPE);
        return supportedTypes;
    }

    private void includeAndExcludeFields(Set<String> fields, Set<FieldSelection> fieldSelection) {
        FetchSourceContext analyzedFields = config.getAnalyzedFields();
        if (analyzedFields == null) {
            return;
        }

        checkIncludesExcludesAreNotObjects(analyzedFields);

        String includes = analyzedFields.includes().length == 0 ? "*" : Strings.arrayToCommaDelimitedString(analyzedFields.includes());
        String excludes = Strings.arrayToCommaDelimitedString(analyzedFields.excludes());

        if (Regex.isMatchAllPattern(includes) && excludes.isEmpty()) {
            return;
        }
        try {
            // If the inclusion set does not match anything, that means the user's desired fields cannot be found in
            // the collection of supported field types. We should let the user know.
            Set<String> includedSet = NameResolver.newUnaliased(fields,
                (ex) -> new ResourceNotFoundException(
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_FIELD_FILTER, ex)))
                .expand(includes, false);
            // If the exclusion set does not match anything, that means the fields are already not present
            // no need to raise if nothing matched
            Set<String> excludedSet = NameResolver.newUnaliased(fieldCapabilitiesResponse.get().keySet(),
                (ex) -> new ResourceNotFoundException(
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_FIELD_FILTER, ex)))
                .expand(excludes, true);

            applyIncludesExcludes(fields, includedSet, excludedSet, fieldSelection);
        } catch (ResourceNotFoundException ex) {
            // Re-wrap our exception so that we throw the same exception type when there are no fields.
            throw ExceptionsHelper.badRequestException(ex.getMessage());
        }
    }

    private void checkIncludesExcludesAreNotObjects(FetchSourceContext analyzedFields) {
        List<String> objectFields = Stream.concat(Arrays.stream(analyzedFields.includes()), Arrays.stream(analyzedFields.excludes()))
            .filter(field -> isObject(getMappingTypes(field)))
            .collect(Collectors.toList());
        if (objectFields.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("{} must not include or exclude object fields: {}",
                DataFrameAnalyticsConfig.ANALYZED_FIELDS.getPreferredName(), objectFields);
        }
    }

    private void applyIncludesExcludes(Set<String> fields, Set<String> includes, Set<String> excludes,
                                       Set<FieldSelection> fieldSelection) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            if (includes.contains(field)) {
                if (IGNORE_FIELDS.contains(field)) {
                    throw ExceptionsHelper.badRequestException("field [{}] cannot be analyzed", field);
                }
                if (excludes.contains(field)) {
                    fieldsIterator.remove();
                    addExcludedField(field, "field in excludes list", fieldSelection);
                }
            } else {
                fieldsIterator.remove();
                if (hasCompatibleType(field)) {
                    addExcludedField(field, "field not in includes list", fieldSelection);
                } else {
                    addExcludedField(field, "unsupported type; supported types are " + getSupportedTypes(), fieldSelection);
                }
            }
        }
    }

    private void checkFieldsHaveCompatibleTypes(Set<String> fields) {
        for (String field : fields) {
            Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(field);
            if (fieldCaps == null) {
                throw ExceptionsHelper.badRequestException("no mappings could be found for field [{}]", field);
            }

            if (hasCompatibleType(field) == false) {
                throw ExceptionsHelper.badRequestException("field [{}] has unsupported type {}. Supported types are {}.", field,
                    fieldCaps.keySet(), getSupportedTypes());
            }
        }
    }

    private void checkRequiredFields(Set<String> fields) {
        List<RequiredField> requiredFields = config.getAnalysis().getRequiredFields();
        for (RequiredField requiredField : requiredFields) {
            Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(requiredField.getName());
            if (fields.contains(requiredField.getName()) == false || fieldCaps == null || fieldCaps.isEmpty()) {
                List<String> requiredFieldNames = requiredFields.stream().map(RequiredField::getName).collect(Collectors.toList());
                throw ExceptionsHelper.badRequestException("required field [{}] is missing; analysis requires fields {}",
                    requiredField.getName(), requiredFieldNames);
            }
            Set<String> fieldTypes = fieldCaps.keySet();
            if (requiredField.getTypes().containsAll(fieldTypes) == false) {
                throw ExceptionsHelper.badRequestException("invalid types {} for required field [{}]; expected types are {}",
                    fieldTypes, requiredField.getName(), requiredField.getTypes());
            }
        }
    }

    private void checkFieldsWithCardinalityLimit() {
        for (FieldCardinalityConstraint constraint : config.getAnalysis().getFieldCardinalityConstraints()) {
            constraint.check(cardinalitiesForFieldsWithConstraints.get(constraint.getField()));
        }
    }

    private ExtractedFields detectExtractedFields(Set<String> fields, Set<FieldSelection> fieldSelection) {
        ExtractedFields extractedFields = ExtractedFields.build(fields, Collections.emptySet(), fieldCapabilitiesResponse,
            cardinalitiesForFieldsWithConstraints);
        boolean preferSource = extractedFields.getDocValueFields().size() > docValueFieldsLimit;
        extractedFields = deduplicateMultiFields(extractedFields, preferSource, fieldSelection);
        if (preferSource) {
            extractedFields = fetchFromSourceIfSupported(extractedFields);
            if (extractedFields.getDocValueFields().size() > docValueFieldsLimit) {
                throw ExceptionsHelper.badRequestException("[{}] fields must be retrieved from doc_values but the limit is [{}]; " +
                        "please adjust the index level setting [{}]", extractedFields.getDocValueFields().size(), docValueFieldsLimit,
                    IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey());
            }
        }
        extractedFields = fetchBooleanFieldsAsIntegers(extractedFields);
        return extractedFields;
    }

    private ExtractedFields deduplicateMultiFields(ExtractedFields extractedFields, boolean preferSource,
                                                   Set<FieldSelection> fieldSelection) {
        Set<String> requiredFields = config.getAnalysis().getRequiredFields().stream().map(RequiredField::getName)
            .collect(Collectors.toSet());
        Map<String, ExtractedField> nameOrParentToField = new LinkedHashMap<>();
        for (ExtractedField currentField : extractedFields.getAllFields()) {
            String nameOrParent = currentField.isMultiField() ? currentField.getParentField() : currentField.getName();
            ExtractedField existingField = nameOrParentToField.putIfAbsent(nameOrParent, currentField);
            if (existingField != null) {
                ExtractedField parent = currentField.isMultiField() ? existingField : currentField;
                ExtractedField multiField = currentField.isMultiField() ? currentField : existingField;
                nameOrParentToField.put(nameOrParent,
                    chooseMultiFieldOrParent(preferSource, requiredFields, parent, multiField, fieldSelection));
            }
        }
        return new ExtractedFields(new ArrayList<>(nameOrParentToField.values()), cardinalitiesForFieldsWithConstraints);
    }

    private ExtractedField chooseMultiFieldOrParent(boolean preferSource, Set<String> requiredFields, ExtractedField parent,
                                                    ExtractedField multiField, Set<FieldSelection> fieldSelection) {
        // Check requirements first
        if (requiredFields.contains(parent.getName())) {
            addExcludedField(multiField.getName(), "[" + parent.getName() + "] is required instead", fieldSelection);
            return parent;
        }
        if (requiredFields.contains(multiField.getName())) {
            addExcludedField(parent.getName(), "[" + multiField.getName() + "] is required instead", fieldSelection);
            return multiField;
        }

        // If both are multi-fields it means there are several. In this case parent is the previous multi-field
        // we selected. We'll just keep that.
        if (parent.isMultiField() && multiField.isMultiField()) {
            addExcludedField(multiField.getName(), "[" + parent.getName() + "] came first", fieldSelection);
            return parent;
        }

        // If we prefer source only the parent may support it. If it does we pick it immediately.
        if (preferSource && parent.supportsFromSource()) {
            addExcludedField(multiField.getName(), "[" + parent.getName() + "] is preferred because it supports fetching from source",
                fieldSelection);
            return parent;
        }

        // If any of the two is a doc_value field let's prefer it as it'd support aggregations.
        // We check the parent first as it'd be a shorter field name.
        if (parent.getMethod() == ExtractedField.Method.DOC_VALUE) {
            addExcludedField(multiField.getName(), "[" + parent.getName() + "] is preferred because it is aggregatable", fieldSelection);
            return parent;
        }
        if (multiField.getMethod() == ExtractedField.Method.DOC_VALUE) {
            addExcludedField(parent.getName(), "[" + multiField.getName() + "] is preferred because it is aggregatable", fieldSelection);
            return multiField;
        }

        // None is aggregatable. Let's pick the parent for its shorter name.
        addExcludedField(multiField.getName(), "[" + parent.getName() + "] is preferred because none of the multi-fields are aggregatable",
            fieldSelection);
        return parent;
    }

    private ExtractedFields fetchFromSourceIfSupported(ExtractedFields extractedFields) {
        List<ExtractedField> adjusted = new ArrayList<>(extractedFields.getAllFields().size());
        for (ExtractedField field : extractedFields.getAllFields()) {
            adjusted.add(field.supportsFromSource() ? field.newFromSource() : field);
        }
        return new ExtractedFields(adjusted, cardinalitiesForFieldsWithConstraints);
    }

    private ExtractedFields fetchBooleanFieldsAsIntegers(ExtractedFields extractedFields) {
        List<ExtractedField> adjusted = new ArrayList<>(extractedFields.getAllFields().size());
        for (ExtractedField field : extractedFields.getAllFields()) {
            if (isBoolean(field.getTypes())) {
                // We convert boolean fields to integers with values 0, 1 as this is the preferred
                // way to consume such features in the analytics process regardless of:
                //  - analysis type
                //  - whether or not the field is categorical
                //  - whether or not the field is a dependent variable
                adjusted.add(ExtractedFields.applyBooleanMapping(field));
            } else {
                adjusted.add(field);
            }
        }
        return new ExtractedFields(adjusted, cardinalitiesForFieldsWithConstraints);
    }

    private void addIncludedFields(ExtractedFields extractedFields, Set<FieldSelection> fieldSelection) {
        Set<String> requiredFields = config.getAnalysis().getRequiredFields().stream().map(RequiredField::getName)
            .collect(Collectors.toSet());
        Set<String> categoricalFields = getCategoricalFields(extractedFields, config.getAnalysis());
        for (ExtractedField includedField : extractedFields.getAllFields()) {
            FieldSelection.FeatureType featureType = categoricalFields.contains(includedField.getName()) ?
                FieldSelection.FeatureType.CATEGORICAL : FieldSelection.FeatureType.NUMERICAL;
            fieldSelection.add(FieldSelection.included(includedField.getName(), includedField.getTypes(),
                requiredFields.contains(includedField.getName()), featureType));
        }
    }

    static Set<String> getCategoricalFields(ExtractedFields extractedFields, DataFrameAnalysis analysis) {
        return extractedFields.getAllFields().stream()
            .filter(extractedField -> analysis.getAllowedCategoricalTypes(extractedField.getName())
                .containsAll(extractedField.getTypes()))
            .map(ExtractedField::getName)
            .collect(Collectors.toUnmodifiableSet());
    }

    private static boolean isBoolean(Set<String> types) {
        return types.size() == 1 && types.contains(BooleanFieldMapper.CONTENT_TYPE);
    }

    private boolean isObject(Set<String> types) {
        return types.size() == 1 && types.contains(ObjectMapper.CONTENT_TYPE);
    }
}
