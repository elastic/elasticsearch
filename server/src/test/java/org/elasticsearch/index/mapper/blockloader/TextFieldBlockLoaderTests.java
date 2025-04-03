/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.blockloader;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.BlockLoaderTestCase;
import org.elasticsearch.logsdb.datageneration.FieldType;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class TextFieldBlockLoaderTests extends BlockLoaderTestCase {
    public TextFieldBlockLoaderTests(Params params) {
        super(FieldType.TEXT.toString(), params);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Object expected(Map<String, Object> fieldMapping, Object value, TestContext testContext) {
        if (fieldMapping.getOrDefault("store", false).equals(true)) {
            return valuesInSourceOrder(value);
        }

        var fields = (Map<String, Object>) fieldMapping.get("fields");
        if (fields != null) {
            var keywordMultiFieldMapping = (Map<String, Object>) fields.get("kwd");
            boolean docValues = hasDocValues(keywordMultiFieldMapping, true);
            boolean index = keywordMultiFieldMapping.getOrDefault("index", true).equals(true);
            boolean store = keywordMultiFieldMapping.getOrDefault("store", false).equals(true);
            Object ignoreAbove = keywordMultiFieldMapping.get("ignore_above");

            // See TextFieldMapper.SyntheticSourceHelper#usingSyntheticSourceDelegate
            // and TextFieldMapper#canUseSyntheticSourceDelegateForLoading().
            boolean usingSyntheticSourceDelegate = docValues || store;
            boolean canUseSyntheticSourceDelegateForLoading = usingSyntheticSourceDelegate && ignoreAbove == null && (index || store);
            if (canUseSyntheticSourceDelegateForLoading) {
                return KeywordFieldBlockLoaderTests.expectedValue(keywordMultiFieldMapping, value, params, testContext);
            }

            // Even if multi-field is not eligible for loading it can still be used to produce synthetic source
            // and then we load from the synthetic source.
            // Synthetic source is actually different from keyword field block loader results
            // because synthetic source includes values exceeding ignore_above and block loader doesn't.
            // TODO ideally this logic should be in some kind of KeywordFieldSyntheticSourceTest that uses same infra as
            // KeywordFieldBlockLoaderTest
            // It is here since KeywordFieldBlockLoaderTest does not really need it
            if (params.syntheticSource() && testContext.forceFallbackSyntheticSource() == false && usingSyntheticSourceDelegate) {
                var nullValue = (String) keywordMultiFieldMapping.get("null_value");

                // Due to how TextFieldMapper#blockReaderDisiLookup works this is complicated.
                // If we are using lookupMatchingAll() then we'll see all docs, generate synthetic source using syntheticSourceDelegate,
                // parse it and see null_value inside.
                // But if we are using lookupFromNorms() we will skip the document (since the text field itself does not exist).
                // Same goes for lookupFromFieldNames().
                boolean textFieldIndexed = (boolean) fieldMapping.getOrDefault("index", true);

                if (value == null) {
                    if (textFieldIndexed == false
                        && nullValue != null
                        && (ignoreAbove == null || nullValue.length() <= (int) ignoreAbove)) {
                        return new BytesRef(nullValue);
                    }

                    return null;
                }

                if (value instanceof String s) {
                    return new BytesRef(s);
                }

                var values = (List<String>) value;

                // See note above about TextFieldMapper#blockReaderDisiLookup.
                if (textFieldIndexed && values.stream().allMatch(Objects::isNull)) {
                    return null;
                }

                var indexed = values.stream()
                    .map(s -> s == null ? nullValue : s)
                    .filter(Objects::nonNull)
                    .filter(s -> ignoreAbove == null || s.length() <= (int) ignoreAbove)
                    .map(BytesRef::new)
                    .collect(Collectors.toList());

                if (store == false) {
                    // using doc_values for synthetic source
                    indexed = new ArrayList<>(new HashSet<>(indexed));
                    indexed.sort(BytesRef::compareTo);
                }

                // ignored values always come last
                List<BytesRef> ignored = ignoreAbove == null
                    ? List.of()
                    : values.stream()
                        .map(s -> s == null ? nullValue : s)
                        .filter(Objects::nonNull)
                        .filter(s -> s.length() > (int) ignoreAbove)
                        .map(BytesRef::new)
                        .toList();

                indexed.addAll(ignored);

                return maybeFoldList(indexed);
            }
        }

        // Loading from _ignored_source or stored _source
        return valuesInSourceOrder(value);
    }

    @SuppressWarnings("unchecked")
    private Object valuesInSourceOrder(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof String s) {
            return new BytesRef(s);
        }

        var resultList = ((List<String>) value).stream().filter(Objects::nonNull).map(BytesRef::new).toList();
        return maybeFoldList(resultList);
    }
}
