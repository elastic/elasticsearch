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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
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
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExtractedFieldsDetector {

    private static final Logger LOGGER = LogManager.getLogger(ExtractedFieldsDetector.class);

    /**
     * Fields to ignore. These are mostly internal meta fields.
     */
    private static final List<String> IGNORE_FIELDS = Arrays.asList("_id", "_field_names", "_index", "_parent", "_routing", "_seq_no",
        "_source", "_type", "_uid", "_version", "_feature", "_ignored", DataFrameAnalyticsIndex.ID_COPY);

    public static final Set<String> CATEGORICAL_TYPES = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("text", "keyword", "ip")));

    private static final Set<String> NUMERICAL_TYPES;

    static {
        Set<String> numericalTypes = Stream.of(NumberFieldMapper.NumberType.values())
            .map(NumberFieldMapper.NumberType::typeName)
            .collect(Collectors.toSet());
        numericalTypes.add("scaled_float");
        NUMERICAL_TYPES = Collections.unmodifiableSet(numericalTypes);
    }

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
        Set<String> fields = new HashSet<>(fieldCapabilitiesResponse.get().keySet());
        fields.removeAll(IGNORE_FIELDS);
        removeFieldsUnderResultsField(fields);
        includeAndExcludeFields(fields);
        removeFieldsWithIncompatibleTypes(fields);
        checkRequiredFieldsArePresent(fields);

        if (fields.isEmpty()) {
            throw ExceptionsHelper.badRequestException("No compatible fields could be detected in index {}", Arrays.toString(index));
        }

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
        return extractedFields;
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
            Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(field);
            if (fieldCaps == null) {
                LOGGER.debug("[{}] Removing field [{}] because it is missing from mappings", config.getId(), field);
                fieldsIterator.remove();
            } else {
                Set<String> fieldTypes = fieldCaps.keySet();
                if (NUMERICAL_TYPES.containsAll(fieldTypes)) {
                    LOGGER.debug("[{}] field [{}] is compatible as it is numerical", config.getId(), field);
                } else if (config.getAnalysis().supportsCategoricalFields() && CATEGORICAL_TYPES.containsAll(fieldTypes)) {
                    LOGGER.debug("[{}] field [{}] is compatible as it is categorical", config.getId(), field);
                } else {
                    LOGGER.debug("[{}] Removing field [{}] because its types are not supported; types {}",
                        config.getId(), field, fieldTypes);
                    fieldsIterator.remove();
                }
            }
        }
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

    private void checkRequiredFieldsArePresent(Set<String> fields) {
        List<String> missingFields = config.getAnalysis().getRequiredFields()
            .stream()
            .filter(f -> fields.contains(f) == false)
            .collect(Collectors.toList());
        if (missingFields.isEmpty() == false) {
            throw ExceptionsHelper.badRequestException("required fields {} are missing", missingFields);
        }
    }

    private ExtractedFields fetchFromSourceIfSupported(ExtractedFields extractedFields) {
        List<ExtractedField> adjusted = new ArrayList<>(extractedFields.getAllFields().size());
        for (ExtractedField field : extractedFields.getDocValueFields()) {
            adjusted.add(field.supportsFromSource() ? field.newFromSource() : field);
        }
        return new ExtractedFields(adjusted);
    }
}
