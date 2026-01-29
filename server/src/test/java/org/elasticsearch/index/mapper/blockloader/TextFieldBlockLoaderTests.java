/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.datageneration.FieldType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TextFieldBlockLoaderTests extends BlockLoaderTestCase {

    private final boolean useBinaryDocValues;

    public TextFieldBlockLoaderTests(
        boolean syntheticSource,
        MappedFieldType.FieldExtractPreference preference,
        boolean useBinaryDocValues
    ) {
        super(FieldType.TEXT.toString(), new Params(syntheticSource, preference));
        this.useBinaryDocValues = useBinaryDocValues;
    }

    @ParametersFactory(argumentFormatting = "syntheticSource=%s, preference=%s, useBinaryDocValues=%s")
    public static List<Object[]> args() {
        List<Object[]> args = new ArrayList<>();
        for (var preference : PREFERENCES) {
            for (boolean syntheticSource : new boolean[] { false, true }) {
                for (boolean useBinaryDocValues : new boolean[] { false, true }) {
                    args.add(new Object[] { syntheticSource, preference, useBinaryDocValues });
                }
            }
        }
        return args;
    }

    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        logger.info("field mapping={}", fieldMapping);
        logger.info("value={}", value);
        logger.info("params={}", params.toString());
        return expectedValue(fieldMapping, value, params, testContext, useBinaryDocValues);
    }

    @SuppressWarnings("unchecked")
    public static Object expectedValue(
        Map<String, Object> fieldMapping,
        Object value,
        Params params,
        TestContext testContext,
        boolean useBinaryDocValues
    ) {
        if (fieldMapping.getOrDefault("store", false).equals(true)) {
            return valuesInSourceOrder(value);
        }

        var fields = (Map<String, Object>) fieldMapping.get("fields");
        if (fields != null) {
            var keywordMultiFieldMapping = (Map<String, Object>) fields.get("subfield_keyword");
            Object normalizer = fields.get("normalizer");
            boolean docValues = hasDocValues(keywordMultiFieldMapping, true);
            boolean store = keywordMultiFieldMapping.getOrDefault("store", false).equals(true);
            Object ignoreAbove = keywordMultiFieldMapping.get("ignore_above");

            // See TextFieldMapper.SyntheticSourceHelper#getKeywordFieldMapperForSyntheticSource
            // and TextFieldMapper#canUseSyntheticSourceDelegateForLoading().
            boolean usingSyntheticSourceDelegate = normalizer == null && (docValues || store);
            boolean canUseSyntheticSourceDelegateForLoading = usingSyntheticSourceDelegate && ignoreAbove == null;
            if (canUseSyntheticSourceDelegateForLoading) {
                return KeywordFieldBlockLoaderTests.expectedValue(keywordMultiFieldMapping, value, params, testContext);
            }

            // Even if multi field is not eligible for loading it can still be used to produce synthetic source
            // and then we load from the synthetic source.
            // Synthetic source is actually different from keyword field block loader results
            // because synthetic source includes values exceeding ignore_above and block loader doesn't.
            // TODO ideally this logic should be in some kind of KeywordFieldSyntheticSourceTest that uses same infra as
            // KeywordFieldBlockLoaderTest
            // It is here since KeywordFieldBlockLoaderTest does not really need it
            if (params.syntheticSource() && testContext.forceFallbackSyntheticSource() == false && usingSyntheticSourceDelegate) {
                var nullValue = (String) keywordMultiFieldMapping.get("null_value");

                if (value == null) {
                    if (nullValue != null && nullValue.length() <= (int) ignoreAbove) {
                        return new BytesRef(nullValue);
                    }

                    return null;
                }

                if (value instanceof String s) {
                    return new BytesRef(s);
                }

                var values = (List<String>) value;
                var indexed = values.stream()
                    .map(s -> s == null ? nullValue : s)
                    .filter(Objects::nonNull)
                    .filter(s -> s.length() <= (int) ignoreAbove)
                    .map(BytesRef::new)
                    .collect(Collectors.toList());

                String ssk = (String) keywordMultiFieldMapping.get("synthetic_source_keep");
                if (store == false && "arrays".equals(ssk) == false) {
                    // using doc_values for synthetic source
                    indexed = new ArrayList<>(new HashSet<>(indexed));
                    indexed.sort(BytesRef::compareTo);
                }

                // ignored values always come last
                List<BytesRef> ignored = values.stream()
                    .map(s -> s == null ? nullValue : s)
                    .filter(Objects::nonNull)
                    .filter(s -> s.length() > (int) ignoreAbove)
                    .map(BytesRef::new)
                    .toList();

                indexed.addAll(ignored);

                return maybeFoldList(indexed);
            }
        }

        // Loading from binary doc values
        if (params.syntheticSource() && useBinaryDocValues) {
            return valuesInSortedOrder(value);
        }

        // Loading from stored field, _ignored_source or stored _source
        return valuesInSourceOrder(value);
    }

    @SuppressWarnings("unchecked")
    private static Object valuesInSortedOrder(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String s) {
            return new BytesRef(s);
        }

        var resultList = ((List<String>) value).stream().filter(Objects::nonNull).map(BytesRef::new).sorted().toList();
        return maybeFoldList(resultList);
    }

    @SuppressWarnings("unchecked")
    private static Object valuesInSourceOrder(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String s) {
            return new BytesRef(s);
        }

        var resultList = ((List<String>) value).stream().filter(Objects::nonNull).map(BytesRef::new).toList();
        return maybeFoldList(resultList);
    }

    @Override
    protected Settings.Builder getSettingsForParams() {
        var builder = Settings.builder();
        if (params.syntheticSource()) {
            builder.put("index.mapping.source.mode", "synthetic");
            if (useBinaryDocValues) {
                builder.put(IndexSettings.USE_TIME_SERIES_DOC_VALUES_FORMAT_SETTING.getKey(), true);
            }
        }
        return builder;
    }
}
