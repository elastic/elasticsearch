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
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.RequiredField;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Types;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.NameResolver;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedField;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedFields;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsIndex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class ExtractedFieldsDetector {

    private static final Logger LOGGER = LogManager.getLogger(ExtractedFieldsDetector.class);

    /**
     * Fields to ignore. These are mostly internal meta fields.
     */
    private static final List<String> IGNORE_FIELDS = Arrays.asList("_id", "_field_names", "_index", "_parent", "_routing", "_seq_no",
        "_source", "_type", "_uid", "_version", "_feature", "_ignored", DataFrameAnalyticsIndex.ID_COPY);

    private final String[] index;
    private final DataFrameAnalyticsConfig config;
    private final String resultsField;
    private final boolean isTaskRestarting;
    private final int docValueFieldsLimit;
    private final FieldCapabilitiesResponse fieldCapabilitiesResponse;

    ExtractedFieldsDetector(String[] index, DataFrameAnalyticsConfig config, String resultsField, boolean isTaskRestarting,
                            int docValueFieldsLimit, FieldCapabilitiesResponse fieldCapabilitiesResponse) {
        this.index = Objects.requireNonNull(index);
        this.config = Objects.requireNonNull(config);
        this.resultsField = resultsField;
        this.isTaskRestarting = isTaskRestarting;
        this.docValueFieldsLimit = docValueFieldsLimit;
        this.fieldCapabilitiesResponse = Objects.requireNonNull(fieldCapabilitiesResponse);
    }

    public ExtractedFields detect() {
        Set<String> fields = getIncludedFields();

        if (fields.isEmpty()) {
            throw ExceptionsHelper.badRequestException("No compatible fields could be detected in index {}. Supported types are {}.",
                Arrays.toString(index),
                getSupportedTypes());
        }

        checkNoIgnoredFields(fields);
        checkFieldsHaveCompatibleTypes(fields);
        checkRequiredFields(fields);
        return detectExtractedFields(fields);
    }

    private Set<String> getIncludedFields() {
        Set<String> fields = new HashSet<>(fieldCapabilitiesResponse.get().keySet());
        removeFieldsUnderResultsField(fields);
        FetchSourceContext analyzedFields = config.getAnalyzedFields();

        // If the user has not explicitly included fields we'll include all compatible fields
        if (analyzedFields == null || analyzedFields.includes().length == 0) {
            fields.removeAll(IGNORE_FIELDS);
            removeFieldsWithIncompatibleTypes(fields);
        }
        includeAndExcludeFields(fields);
        return fields;
    }

    private void removeFieldsUnderResultsField(Set<String> fields) {
        if (resultsField == null) {
            return;
        }
        checkResultsFieldIsNotPresent();
        // Ignore fields under the results object
        fields.removeIf(field -> field.startsWith(resultsField + "."));
    }

    private void checkResultsFieldIsNotPresent() {
        // If the task is restarting we do not mind the index containing the results field, we will overwrite all docs
        if (isTaskRestarting) {
            return;
        }

        Map<String, FieldCapabilities> indexToFieldCaps = fieldCapabilitiesResponse.getField(resultsField);
        if (indexToFieldCaps != null && indexToFieldCaps.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException(
                "A field that matches the {}.{} [{}] already exists; please set a different {}",
                DataFrameAnalyticsConfig.DEST.getPreferredName(),
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName(),
                resultsField,
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName());
        }
    }

    private void removeFieldsWithIncompatibleTypes(Set<String> fields) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            if (hasCompatibleType(field) == false) {
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

    private void includeAndExcludeFields(Set<String> fields) {
        FetchSourceContext analyzedFields = config.getAnalyzedFields();
        if (analyzedFields == null) {
            return;
        }
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
            Set<String> excludedSet = NameResolver.newUnaliased(fields,
                (ex) -> new ResourceNotFoundException(
                    Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_FIELD_FILTER, ex)))
                .expand(excludes, true);

            fields.retainAll(includedSet);
            fields.removeAll(excludedSet);
        } catch (ResourceNotFoundException ex) {
            // Re-wrap our exception so that we throw the same exception type when there are no fields.
            throw ExceptionsHelper.badRequestException(ex.getMessage());
        }
    }

    private void checkNoIgnoredFields(Set<String> fields) {
        Optional<String> ignoreField = IGNORE_FIELDS.stream().filter(fields::contains).findFirst();
        if (ignoreField.isPresent()) {
            throw ExceptionsHelper.badRequestException("field [{}] cannot be analyzed", ignoreField.get());
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

    private ExtractedFields detectExtractedFields(Set<String> fields) {
        List<String> sortedFields = new ArrayList<>(fields);
        // We sort the fields to ensure the checksum for each document is deterministic
        Collections.sort(sortedFields);
        ExtractedFields extractedFields = ExtractedFields.build(sortedFields, Collections.emptySet(), fieldCapabilitiesResponse);
        if (extractedFields.getDocValueFields().size() > docValueFieldsLimit) {
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

    private ExtractedFields fetchFromSourceIfSupported(ExtractedFields extractedFields) {
        List<ExtractedField> adjusted = new ArrayList<>(extractedFields.getAllFields().size());
        for (ExtractedField field : extractedFields.getDocValueFields()) {
            adjusted.add(field.supportsFromSource() ? field.newFromSource() : field);
        }
        return new ExtractedFields(adjusted);
    }

    private ExtractedFields fetchBooleanFieldsAsIntegers(ExtractedFields extractedFields) {
        List<ExtractedField> adjusted = new ArrayList<>(extractedFields.getAllFields().size());
        for (ExtractedField field : extractedFields.getAllFields()) {
            if (isBoolean(field.getTypes())) {
                adjusted.add(new BooleanAsInteger(field));
            } else {
                adjusted.add(field);
            }
        }
        return new ExtractedFields(adjusted);
    }

    private static boolean isBoolean(Set<String> types) {
        return types.size() == 1 && types.contains(BooleanFieldMapper.CONTENT_TYPE);
    }

    /**
     * We convert boolean fields to integers with values 0, 1 as this is the preferred
     * way to consume such features in the analytics process.
     */
    private static class BooleanAsInteger extends ExtractedField {

        protected BooleanAsInteger(ExtractedField field) {
            super(field.getAlias(), field.getName(), Collections.singleton(BooleanFieldMapper.CONTENT_TYPE), ExtractionMethod.DOC_VALUE);
        }

        @Override
        public Object[] value(SearchHit hit) {
            DocumentField keyValue = hit.field(name);
            if (keyValue != null) {
                List<Object> values = keyValue.getValues().stream().map(v -> Boolean.TRUE.equals(v) ? 1 : 0).collect(Collectors.toList());
                return values.toArray(new Object[0]);
            }
            return new Object[0];
        }

        @Override
        public boolean supportsFromSource() {
            return false;
        }
    }
}
