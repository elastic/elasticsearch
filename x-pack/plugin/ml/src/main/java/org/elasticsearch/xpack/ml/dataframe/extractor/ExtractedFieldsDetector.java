/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

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

    /**
     * Fields to ignore. These are mostly internal meta fields.
     */
    private static final List<String> IGNORE_FIELDS = Arrays.asList("_id", "_field_names", "_index", "_parent", "_routing", "_seq_no",
        "_source", "_type", "_uid", "_version", "_feature", "_ignored");

    /**
     * The types supported by data frames
     */
    private static final Set<String> COMPATIBLE_FIELD_TYPES;

    static {
        Set<String> compatibleTypes = Stream.of(NumberFieldMapper.NumberType.values())
            .map(NumberFieldMapper.NumberType::typeName)
            .collect(Collectors.toSet());
        compatibleTypes.add("scaled_float"); // have to add manually since scaled_float is in a module

        COMPATIBLE_FIELD_TYPES = Collections.unmodifiableSet(compatibleTypes);
    }

    private final String index;
    private final DataFrameAnalyticsConfig config;
    private final boolean isTaskRestarting;
    private final int docValueFieldsLimit;
    private final FieldCapabilitiesResponse fieldCapabilitiesResponse;

    ExtractedFieldsDetector(String index, DataFrameAnalyticsConfig config, boolean isTaskRestarting, int docValueFieldsLimit,
                            FieldCapabilitiesResponse fieldCapabilitiesResponse) {
        this.index = Objects.requireNonNull(index);
        this.config = Objects.requireNonNull(config);
        this.isTaskRestarting = isTaskRestarting;
        this.docValueFieldsLimit = docValueFieldsLimit;
        this.fieldCapabilitiesResponse = Objects.requireNonNull(fieldCapabilitiesResponse);
    }

    public ExtractedFields detect() {
        Set<String> fields = new HashSet<>(fieldCapabilitiesResponse.get().keySet());
        fields.removeAll(IGNORE_FIELDS);

        checkResultsFieldIsNotPresent(fields, index);

        // Ignore fields under the results object
        fields.removeIf(field -> field.startsWith(config.getDest().getResultsField() + "."));

        removeFieldsWithIncompatibleTypes(fields);
        includeAndExcludeFields(fields, index);
        List<String> sortedFields = new ArrayList<>(fields);
        // We sort the fields to ensure the checksum for each document is deterministic
        Collections.sort(sortedFields);
        ExtractedFields extractedFields = ExtractedFields.build(sortedFields, Collections.emptySet(), fieldCapabilitiesResponse)
            .filterFields(ExtractedField.ExtractionMethod.DOC_VALUE);
        if (extractedFields.getAllFields().isEmpty()) {
            throw ExceptionsHelper.badRequestException("No compatible fields could be detected in index [{}]", index);
        }
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

    private void checkResultsFieldIsNotPresent(Set<String> fields, String index) {
        // If the task is restarting we do not mind the index containing the results field, we will overwrite all docs
        if (isTaskRestarting == false && fields.contains(config.getDest().getResultsField())) {
            throw ExceptionsHelper.badRequestException("Index [{}] already has a field that matches the {}.{} [{}];" +
                    " please set a different {}", index, DataFrameAnalyticsConfig.DEST.getPreferredName(),
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName(), config.getDest().getResultsField(),
                DataFrameAnalyticsDest.RESULTS_FIELD.getPreferredName());
        }
    }

    private void removeFieldsWithIncompatibleTypes(Set<String> fields) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(field);
            if (fieldCaps == null || COMPATIBLE_FIELD_TYPES.containsAll(fieldCaps.keySet()) == false) {
                fieldsIterator.remove();
            }
        }
    }

    private void includeAndExcludeFields(Set<String> fields, String index) {
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
                (ex) -> new ResourceNotFoundException(Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_FIELD_FILTER, index, ex)))
                .expand(includes, false);
            // If the exclusion set does not match anything, that means the fields are already not present
            // no need to raise if nothing matched
            Set<String> excludedSet = NameResolver.newUnaliased(fields,
                (ex) -> new ResourceNotFoundException(Messages.getMessage(Messages.DATA_FRAME_ANALYTICS_BAD_FIELD_FILTER, index, ex)))
                .expand(excludes, true);

            fields.retainAll(includedSet);
            fields.removeAll(excludedSet);
        } catch (ResourceNotFoundException ex) {
            // Re-wrap our exception so that we throw the same exception type when there are no fields.
            throw ExceptionsHelper.badRequestException(ex.getMessage());
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
