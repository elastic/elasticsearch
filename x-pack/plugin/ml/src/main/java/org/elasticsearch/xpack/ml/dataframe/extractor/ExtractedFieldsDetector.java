/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.NestedObjectMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.FieldCardinalityConstraint;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RequiredField;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Types;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NameResolver;
import org.elasticsearch.xpack.ml.dataframe.DestinationIndex;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.ProcessedField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtractedFieldsDetector {

    private static final Logger LOGGER = LogManager.getLogger(ExtractedFieldsDetector.class);

    /**
     * Internal fields to ignore.
     */
    private static final List<String> IGNORE_FIELDS = Collections.singletonList(DestinationIndex.INCREMENTAL_ID);

    private final DataFrameAnalyticsConfig config;
    private final int docValueFieldsLimit;
    private final FieldCapabilitiesResponse fieldCapabilitiesResponse;
    private final Map<String, Long> cardinalitiesForFieldsWithConstraints;
    private final List<String> topNestedFieldPrefixes;

    ExtractedFieldsDetector(
        DataFrameAnalyticsConfig config,
        int docValueFieldsLimit,
        FieldCapabilitiesResponse fieldCapabilitiesResponse,
        Map<String, Long> cardinalitiesForFieldsWithConstraints
    ) {
        this.config = Objects.requireNonNull(config);
        this.docValueFieldsLimit = docValueFieldsLimit;
        this.fieldCapabilitiesResponse = Objects.requireNonNull(fieldCapabilitiesResponse);
        this.cardinalitiesForFieldsWithConstraints = Objects.requireNonNull(cardinalitiesForFieldsWithConstraints);
        this.topNestedFieldPrefixes = findTopNestedFieldPrefixes(fieldCapabilitiesResponse);
    }

    private List<String> findTopNestedFieldPrefixes(FieldCapabilitiesResponse response) {
        List<String> sortedNestedFieldPrefixes = response.get()
            .keySet()
            .stream()
            .filter(field -> isNested(getMappingTypes(field)))
            .map(field -> field + ".")
            .sorted()
            .collect(Collectors.toList());
        Iterator<String> iterator = sortedNestedFieldPrefixes.iterator();
        String previousNestedFieldPrefix = null;
        while (iterator.hasNext()) {
            String nestedFieldPrefix = iterator.next();
            if (previousNestedFieldPrefix != null && nestedFieldPrefix.startsWith(previousNestedFieldPrefix)) {
                iterator.remove();
            } else {
                previousNestedFieldPrefix = nestedFieldPrefix;
            }
        }
        return Collections.unmodifiableList(sortedNestedFieldPrefixes);
    }

    public Tuple<ExtractedFields, List<FieldSelection>> detect() {
        List<ProcessedField> processedFields = extractFeatureProcessors().stream().map(ProcessedField::new).collect(Collectors.toList());
        TreeSet<FieldSelection> fieldSelection = new TreeSet<>(Comparator.comparing(FieldSelection::getName));
        Set<String> fields = getIncludedFields(
            fieldSelection,
            processedFields.stream().map(ProcessedField::getInputFieldNames).flatMap(List::stream).collect(Collectors.toSet())
        );
        checkFieldsHaveCompatibleTypes(fields);
        checkRequiredFields(fields);
        checkFieldsWithCardinalityLimit();
        ExtractedFields extractedFields = detectExtractedFields(fields, fieldSelection, processedFields);
        addIncludedFields(extractedFields, fieldSelection);

        checkOutputFeatureUniqueness(processedFields, fields);

        return Tuple.tuple(extractedFields, Collections.unmodifiableList(new ArrayList<>(fieldSelection)));
    }

    private Set<String> getIncludedFields(Set<FieldSelection> fieldSelection, Set<String> requiredFieldsForProcessors) {
        validateFieldsRequireForProcessors(requiredFieldsForProcessors);
        Set<String> fields = new TreeSet<>();
        // filter metadata field
        fieldCapabilitiesResponse.get()
            .keySet()
            .stream()
            .filter(f -> fieldCapabilitiesResponse.isMetadataField(f) == false && IGNORE_FIELDS.contains(f) == false)
            .forEach(fields::add);
        removeFieldsUnderResultsField(fields);
        removeObjects(fields);
        applySourceFiltering(fields);
        if (fields.containsAll(requiredFieldsForProcessors) == false) {
            throw ExceptionsHelper.badRequestException(
                "fields {} required by field_processors are not included in source filtering.",
                Sets.difference(requiredFieldsForProcessors, fields)
            );
        }
        FetchSourceContext analyzedFields = config.getAnalyzedFields();

        // If the user has not explicitly included fields we'll include all compatible fields
        if (analyzedFields == null || analyzedFields.includes().length == 0) {
            removeFieldsWithIncompatibleTypes(fields, fieldSelection);
        }
        includeAndExcludeFields(fields, fieldSelection);
        if (fields.containsAll(requiredFieldsForProcessors) == false) {
            throw ExceptionsHelper.badRequestException(
                "fields {} required by field_processors are not included in the analyzed_fields.",
                Sets.difference(requiredFieldsForProcessors, fields)
            );
        }

        return fields;
    }

    private void validateFieldsRequireForProcessors(Set<String> processorFields) {
        Set<String> fieldsForProcessor = new HashSet<>(processorFields);
        removeFieldsUnderResultsField(fieldsForProcessor);
        if (fieldsForProcessor.size() < processorFields.size()) {
            throw ExceptionsHelper.badRequestException(
                "fields contained in results field [{}] cannot be used in a feature_processor",
                config.getDest().getResultsField()
            );
        }
        removeObjects(fieldsForProcessor);
        if (fieldsForProcessor.size() < processorFields.size()) {
            throw ExceptionsHelper.badRequestException("fields for feature_processors must not be objects or nested");
        }
        for (String field : fieldsForProcessor) {
            Optional<String> matchingNestedFieldPattern = findMatchingNestedFieldPattern(field);
            if (matchingNestedFieldPattern.isPresent()) {
                throw ExceptionsHelper.badRequestException(
                    "nested fields [{}] cannot be used in a feature_processor",
                    matchingNestedFieldPattern.get()
                );
            }
        }
        Collection<String> errorFields = new ArrayList<>();
        for (String fieldName : fieldsForProcessor) {
            if (fieldCapabilitiesResponse.isMetadataField(fieldName) || IGNORE_FIELDS.contains(fieldName)) {
                errorFields.add(fieldName);
            }
        }
        if (errorFields.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("the following fields cannot be used in feature_processors {}", errorFields);
        }
        List<String> fieldsMissingInMapping = processorFields.stream()
            .filter(f -> fieldCapabilitiesResponse.get().containsKey(f) == false)
            .collect(Collectors.toList());
        if (fieldsMissingInMapping.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "the fields {} were not found in the field capabilities of the source indices [{}]. "
                    + "Fields must exist and be mapped to be used in feature_processors.",
                fieldsMissingInMapping,
                Strings.arrayToCommaDelimitedString(config.getSource().getIndex())
            );
        }
        List<String> processedRequiredFields = config.getAnalysis()
            .getRequiredFields()
            .stream()
            .map(RequiredField::getName)
            .filter(processorFields::contains)
            .collect(Collectors.toList());
        if (processedRequiredFields.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "required analysis fields {} cannot be used in a feature_processor",
                processedRequiredFields
            );
        }
    }

    private void removeFieldsUnderResultsField(Set<String> fields) {
        final String resultsFieldPrefix = config.getDest().getResultsField() + ".";
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            if (field.startsWith(resultsFieldPrefix)) {
                fieldsIterator.remove();
            }
        }
        fields.removeIf(field -> field.startsWith(resultsFieldPrefix));
    }

    private void removeObjects(Set<String> fields) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            Set<String> types = getMappingTypes(field);
            if (isObject(types) || isNested(types)) {
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

    private void addExcludedNestedPattern(String pattern, Set<FieldSelection> fieldSelection) {
        fieldSelection.add(
            FieldSelection.excluded(pattern, Collections.singleton(NestedObjectMapper.CONTENT_TYPE), "nested fields are not supported")
        );
    }

    private Set<String> getMappingTypes(String field) {
        Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(field);
        return fieldCaps == null ? Collections.emptySet() : fieldCaps.keySet();
    }

    private void removeFieldsWithIncompatibleTypes(Set<String> fields, Set<FieldSelection> fieldSelection) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            Optional<String> matchingNestedFieldPattern = findMatchingNestedFieldPattern(field);
            if (matchingNestedFieldPattern.isPresent()) {
                addExcludedNestedPattern(matchingNestedFieldPattern.get(), fieldSelection);
                fieldsIterator.remove();
            } else if (hasCompatibleType(field) == false) {
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

    private Optional<String> findMatchingNestedFieldPattern(String field) {
        return topNestedFieldPrefixes.stream().filter(prefix -> field.startsWith(prefix)).map(prefix -> prefix + "*").findFirst();
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
            Set<String> includedSet = expandFields(
                analyzedFields.includes().length == 0 ? new String[] { "*" } : analyzedFields.includes(),
                fields,
                false
            );

            // If the exclusion set does not match anything, that means the fields are already not present
            // no need to raise if nothing matched
            Set<String> excludedSet = expandFields(analyzedFields.excludes(), fieldCapabilitiesResponse.get().keySet(), true);

            applyIncludesExcludes(fields, includedSet, excludedSet, fieldSelection);
        } catch (ResourceNotFoundException ex) {
            // Re-wrap our exception so that we throw the same exception type when there are no fields.
            throw ExceptionsHelper.badRequestException(ex.getMessage());
        }
    }

    private Set<String> expandFields(String[] fields, Set<String> nameset, boolean allowNoMatch) {
        NameResolver nameResolver = NameResolver.newUnaliased(
            nameset,
            (ex) -> new ResourceNotFoundException(Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_FIELD_FILTER, ex))
        );

        Set<String> expanded = new HashSet<>();
        for (String field : fields) {
            expanded.addAll(nameResolver.expand(field, allowNoMatch));
        }
        return expanded;
    }

    private void checkIncludesExcludesAreNotObjects(FetchSourceContext analyzedFields) {
        List<String> objectFields = Stream.concat(Arrays.stream(analyzedFields.includes()), Arrays.stream(analyzedFields.excludes()))
            .filter(field -> isObject(getMappingTypes(field)) || isNested(getMappingTypes(field)))
            .collect(Collectors.toList());
        if (objectFields.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "{} must not include or exclude object or nested fields: {}",
                DataFrameAnalyticsConfig.ANALYZED_FIELDS.getPreferredName(),
                objectFields
            );
        }
    }

    private void applyIncludesExcludes(Set<String> fields, Set<String> includes, Set<String> excludes, Set<FieldSelection> fieldSelection) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            if (includes.contains(field)) {
                if (fieldCapabilitiesResponse.isMetadataField(field) || IGNORE_FIELDS.contains(field)) {
                    throw ExceptionsHelper.badRequestException("field [{}] cannot be analyzed", field);
                }
                if (excludes.contains(field)) {
                    fieldsIterator.remove();
                    addExcludedField(field, "field in excludes list", fieldSelection);
                }
            } else {
                fieldsIterator.remove();
                if (hasCompatibleType(field) == false) {
                    addExcludedField(field, "unsupported type; supported types are " + getSupportedTypes(), fieldSelection);
                } else {
                    Optional<String> matchingNestedFieldPattern = findMatchingNestedFieldPattern(field);
                    if (matchingNestedFieldPattern.isPresent()) {
                        addExcludedNestedPattern(matchingNestedFieldPattern.get(), fieldSelection);
                    } else {
                        addExcludedField(field, "field not in includes list", fieldSelection);
                    }
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
                throw ExceptionsHelper.badRequestException(
                    "field [{}] has unsupported type {}. Supported types are {}.",
                    field,
                    fieldCaps.keySet(),
                    getSupportedTypes()
                );
            }
            Optional<String> matchingNestedFieldPattern = findMatchingNestedFieldPattern(field);
            if (matchingNestedFieldPattern.isPresent()) {
                throw ExceptionsHelper.badRequestException("nested fields [{}] are not supported", matchingNestedFieldPattern.get());
            }
        }
    }

    private void checkRequiredFields(Set<String> fields) {
        List<RequiredField> requiredFields = config.getAnalysis().getRequiredFields();
        for (RequiredField requiredField : requiredFields) {
            Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(requiredField.getName());
            if (fields.contains(requiredField.getName()) == false || fieldCaps == null || fieldCaps.isEmpty()) {
                List<String> requiredFieldNames = requiredFields.stream().map(RequiredField::getName).collect(Collectors.toList());
                throw ExceptionsHelper.badRequestException(
                    "required field [{}] is missing; analysis requires fields {}",
                    requiredField.getName(),
                    requiredFieldNames
                );
            }
            Set<String> fieldTypes = fieldCaps.keySet();
            if (requiredField.getTypes().containsAll(fieldTypes) == false) {
                throw ExceptionsHelper.badRequestException(
                    "invalid types {} for required field [{}]; expected types are {}",
                    fieldTypes,
                    requiredField.getName(),
                    requiredField.getTypes()
                );
            }
        }
    }

    private void checkFieldsWithCardinalityLimit() {
        for (FieldCardinalityConstraint constraint : config.getAnalysis().getFieldCardinalityConstraints()) {
            constraint.check(cardinalitiesForFieldsWithConstraints.get(constraint.getField()));
        }
    }

    private List<PreProcessor> extractFeatureProcessors() {
        final DataFrameAnalysis analysis = config.getAnalysis();
        if (analysis instanceof Classification classification) {
            return classification.getFeatureProcessors();
        } else if (analysis instanceof Regression regression) {
            return regression.getFeatureProcessors();
        }
        return Collections.emptyList();
    }

    private ExtractedFields detectExtractedFields(
        Set<String> fields,
        Set<FieldSelection> fieldSelection,
        List<ProcessedField> processedFields
    ) {
        ExtractedFields extractedFields = ExtractedFields.build(
            fields,
            Collections.emptySet(),
            Collections.emptySet(),
            fieldCapabilitiesResponse,
            cardinalitiesForFieldsWithConstraints,
            processedFields
        );
        boolean preferSource = extractedFields.getDocValueFields().size() > docValueFieldsLimit;
        extractedFields = deduplicateMultiFields(extractedFields, preferSource, fieldSelection);
        if (preferSource) {
            extractedFields = fetchFromSourceIfSupported(extractedFields);
            if (extractedFields.getDocValueFields().size() > docValueFieldsLimit) {
                throw ExceptionsHelper.badRequestException(
                    "[{}] fields must be retrieved from doc_values and this is greater than the configured limit. "
                        + "Please adjust the index level setting [{}]",
                    extractedFields.getDocValueFields().size(),
                    IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING.getKey()
                );
            }
        }
        extractedFields = fetchBooleanFieldsAsIntegers(extractedFields);
        return extractedFields;
    }

    private ExtractedFields deduplicateMultiFields(
        ExtractedFields extractedFields,
        boolean preferSource,
        Set<FieldSelection> fieldSelection
    ) {
        Set<String> requiredFields = config.getAnalysis()
            .getRequiredFields()
            .stream()
            .map(RequiredField::getName)
            .collect(Collectors.toSet());
        Set<String> processorInputFields = extractedFields.getProcessedFieldInputs();
        Map<String, ExtractedField> nameOrParentToField = new LinkedHashMap<>();
        for (ExtractedField currentField : extractedFields.getAllFields()) {
            String nameOrParent = currentField.isMultiField() ? currentField.getParentField() : currentField.getName();
            ExtractedField existingField = nameOrParentToField.putIfAbsent(nameOrParent, currentField);
            if (existingField != null) {
                ExtractedField parent = currentField.isMultiField() ? existingField : currentField;
                ExtractedField multiField = currentField.isMultiField() ? currentField : existingField;
                // If required fields contains parent or multifield and the processor input fields reference the other, that is an error
                // we should not allow processing of data that is required.
                if ((requiredFields.contains(parent.getName()) && processorInputFields.contains(multiField.getName()))
                    || (requiredFields.contains(multiField.getName()) && processorInputFields.contains(parent.getName()))) {
                    throw ExceptionsHelper.badRequestException(
                        "feature_processors cannot be applied to required fields for analysis; multi-field [{}] parent [{}]",
                        multiField.getName(),
                        parent.getName()
                    );
                }
                // If processor input fields have BOTH, we need to keep both.
                if (processorInputFields.contains(parent.getName()) && processorInputFields.contains(multiField.getName())) {
                    throw ExceptionsHelper.badRequestException(
                        "feature_processors refer to both multi-field [{}] and parent [{}]. Please only refer to one or the other",
                        multiField.getName(),
                        parent.getName()
                    );
                }
                nameOrParentToField.put(
                    nameOrParent,
                    chooseMultiFieldOrParent(preferSource, requiredFields, processorInputFields, parent, multiField, fieldSelection)
                );
            }
        }
        return new ExtractedFields(
            new ArrayList<>(nameOrParentToField.values()),
            extractedFields.getProcessedFields(),
            cardinalitiesForFieldsWithConstraints
        );
    }

    private ExtractedField chooseMultiFieldOrParent(
        boolean preferSource,
        Set<String> requiredFields,
        Set<String> processorInputFields,
        ExtractedField parent,
        ExtractedField multiField,
        Set<FieldSelection> fieldSelection
    ) {
        // Check requirements first
        if (requiredFields.contains(parent.getName())) {
            addExcludedField(multiField.getName(), "[" + parent.getName() + "] is required instead", fieldSelection);
            return parent;
        }
        if (requiredFields.contains(multiField.getName())) {
            addExcludedField(parent.getName(), "[" + multiField.getName() + "] is required instead", fieldSelection);
            return multiField;
        }
        // Choose the one required by our processors
        if (processorInputFields.contains(parent.getName())) {
            addExcludedField(
                multiField.getName(),
                "[" + parent.getName() + "] is referenced by feature_processors instead",
                fieldSelection
            );
            return parent;
        }
        if (processorInputFields.contains(multiField.getName())) {
            addExcludedField(
                parent.getName(),
                "[" + multiField.getName() + "] is referenced by feature_processors instead",
                fieldSelection
            );
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
            addExcludedField(
                multiField.getName(),
                "[" + parent.getName() + "] is preferred because it supports fetching from source",
                fieldSelection
            );
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
        addExcludedField(
            multiField.getName(),
            "[" + parent.getName() + "] is preferred because none of the multi-fields are aggregatable",
            fieldSelection
        );
        return parent;
    }

    private ExtractedFields fetchFromSourceIfSupported(ExtractedFields extractedFields) {
        List<ExtractedField> adjusted = new ArrayList<>(extractedFields.getAllFields().size());
        for (ExtractedField field : extractedFields.getAllFields()) {
            adjusted.add(field.supportsFromSource() ? field.newFromSource() : field);
        }
        return new ExtractedFields(adjusted, extractedFields.getProcessedFields(), cardinalitiesForFieldsWithConstraints);
    }

    private ExtractedFields fetchBooleanFieldsAsIntegers(ExtractedFields extractedFields) {
        List<ExtractedField> adjusted = new ArrayList<>(extractedFields.getAllFields().size());
        for (ExtractedField field : extractedFields.getAllFields()) {
            if (isBoolean(field.getTypes())) {
                // We convert boolean fields to integers with values 0, 1 as this is the preferred
                // way to consume such features in the analytics process regardless of:
                // - analysis type
                // - whether or not the field is categorical
                // - whether or not the field is a dependent variable
                adjusted.add(ExtractedFields.applyBooleanMapping(field));
            } else {
                adjusted.add(field);
            }
        }
        return new ExtractedFields(adjusted, extractedFields.getProcessedFields(), cardinalitiesForFieldsWithConstraints);
    }

    private void addIncludedFields(ExtractedFields extractedFields, Set<FieldSelection> fieldSelection) {
        Set<String> requiredFields = config.getAnalysis()
            .getRequiredFields()
            .stream()
            .map(RequiredField::getName)
            .collect(Collectors.toSet());
        Set<String> categoricalFields = getCategoricalInputFields(extractedFields, config.getAnalysis());
        for (ExtractedField includedField : extractedFields.getAllFields()) {
            FieldSelection.FeatureType featureType = categoricalFields.contains(includedField.getName())
                ? FieldSelection.FeatureType.CATEGORICAL
                : FieldSelection.FeatureType.NUMERICAL;
            fieldSelection.add(
                FieldSelection.included(
                    includedField.getName(),
                    includedField.getTypes(),
                    requiredFields.contains(includedField.getName()),
                    featureType
                )
            );
        }
    }

    static void checkOutputFeatureUniqueness(List<ProcessedField> processedFields, Set<String> selectedFields) {
        Set<String> processInputs = processedFields.stream()
            .map(ProcessedField::getInputFieldNames)
            .flatMap(List::stream)
            .collect(Collectors.toSet());
        // All analysis fields that we include that are NOT processed
        // This indicates that they are sent as is
        Set<String> organicFields = Sets.difference(selectedFields, processInputs);

        Set<String> processedFeatures = new HashSet<>();
        Set<String> duplicatedFields = new HashSet<>();
        for (ProcessedField processedField : processedFields) {
            for (String output : processedField.getOutputFieldNames()) {
                if (processedFeatures.add(output) == false) {
                    duplicatedFields.add(output);
                }
            }
        }
        if (duplicatedFields.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "feature_processors must define unique output field names; duplicate fields {}",
                duplicatedFields
            );
        }
        Set<String> duplicateOrganicAndProcessed = Sets.intersection(organicFields, processedFeatures);
        if (duplicateOrganicAndProcessed.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "feature_processors output fields must not include non-processed analysis fields; duplicate fields {}",
                duplicateOrganicAndProcessed
            );
        }
    }

    static Set<String> getCategoricalInputFields(ExtractedFields extractedFields, DataFrameAnalysis analysis) {
        return extractedFields.getAllFields()
            .stream()
            .filter(extractedField -> analysis.getAllowedCategoricalTypes(extractedField.getName()).containsAll(extractedField.getTypes()))
            .map(ExtractedField::getName)
            .collect(Collectors.toSet());
    }

    static Set<String> getCategoricalOutputFields(ExtractedFields extractedFields, DataFrameAnalysis analysis) {
        Set<String> processInputFields = extractedFields.getProcessedFieldInputs();
        Set<String> categoricalFields = extractedFields.getAllFields()
            .stream()
            .filter(extractedField -> analysis.getAllowedCategoricalTypes(extractedField.getName()).containsAll(extractedField.getTypes()))
            .map(ExtractedField::getName)
            .filter(name -> processInputFields.contains(name) == false)
            .collect(Collectors.toSet());

        extractedFields.getProcessedFields().forEach(processedField -> processedField.getOutputFieldNames().forEach(outputField -> {
            if (analysis.getAllowedCategoricalTypes(outputField).containsAll(processedField.getOutputFieldType(outputField))) {
                categoricalFields.add(outputField);
            }
        }));
        return Collections.unmodifiableSet(categoricalFields);
    }

    private static boolean isBoolean(Set<String> types) {
        return types.size() == 1 && types.contains(BooleanFieldMapper.CONTENT_TYPE);
    }

    private static boolean isObject(Set<String> types) {
        return types.size() == 1 && types.contains(ObjectMapper.CONTENT_TYPE);
    }

    private static boolean isNested(Set<String> types) {
        return types.size() == 1 && types.contains(NestedObjectMapper.CONTENT_TYPE);
    }
}
