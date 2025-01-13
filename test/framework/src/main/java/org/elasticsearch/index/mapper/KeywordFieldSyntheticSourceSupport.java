/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KeywordFieldSyntheticSourceSupport implements MapperTestCase.SyntheticSourceSupport {
    private final Integer ignoreAbove;
    private final boolean allIgnored;
    private final boolean store;
    private final boolean docValues;
    private final String nullValue;

    KeywordFieldSyntheticSourceSupport(Integer ignoreAbove, boolean store, String nullValue, boolean useFallbackSyntheticSource) {
        this.ignoreAbove = ignoreAbove;
        this.allIgnored = ignoreAbove != null && LuceneTestCase.rarely();
        this.store = store;
        this.nullValue = nullValue;
        this.docValues = useFallbackSyntheticSource == false || ESTestCase.randomBoolean();
    }

    @Override
    public boolean ignoreAbove() {
        return ignoreAbove != null;
    }

    @Override
    public boolean preservesExactSource() {
        // We opt in into fallback synthetic source implementation
        // if there is nothing else to use, and it preserves exact source data.
        return store == false && docValues == false;
    }

    @Override
    public MapperTestCase.SyntheticSourceExample example(int maxValues) {
        return example(maxValues, false);
    }

    public MapperTestCase.SyntheticSourceExample example(int maxValues, boolean loadBlockFromSource) {
        if (ESTestCase.randomBoolean()) {
            Tuple<String, String> v = generateValue();
            Object sourceValue = preservesExactSource() ? v.v1() : v.v2();
            Object loadBlock = v.v2();
            if (loadBlockFromSource == false && ignoreAbove != null && v.v2().length() > ignoreAbove) {
                loadBlock = null;
            }
            return new MapperTestCase.SyntheticSourceExample(v.v1(), sourceValue, loadBlock, this::mapping);
        }
        List<Tuple<String, String>> values = ESTestCase.randomList(1, maxValues, this::generateValue);
        List<String> in = values.stream().map(Tuple::v1).toList();

        List<String> validValues = new ArrayList<>();
        List<String> ignoredValues = new ArrayList<>();
        values.stream().map(Tuple::v2).forEach(v -> {
            if (ignoreAbove != null && v.length() > ignoreAbove) {
                ignoredValues.add(v);
            } else {
                validValues.add(v);
            }
        });
        List<String> outputFromDocValues = new HashSet<>(validValues).stream().sorted().collect(Collectors.toList());

        Object out;
        if (preservesExactSource()) {
            out = in;
        } else {
            var validValuesInCorrectOrder = store ? validValues : outputFromDocValues;
            var syntheticSourceOutputList = Stream.concat(validValuesInCorrectOrder.stream(), ignoredValues.stream()).toList();
            out = syntheticSourceOutputList.size() == 1 ? syntheticSourceOutputList.get(0) : syntheticSourceOutputList;
        }

        List<String> loadBlock;
        if (loadBlockFromSource) {
            // The block loader infrastructure will never return nulls. Just zap them all.
            loadBlock = in.stream().filter(Objects::nonNull).toList();
        } else if (docValues) {
            loadBlock = List.copyOf(outputFromDocValues);
        } else {
            // Meaning loading from terms.
            loadBlock = List.copyOf(validValues);
        }

        Object loadBlockResult = loadBlock.size() == 1 ? loadBlock.get(0) : loadBlock;
        return new MapperTestCase.SyntheticSourceExample(in, out, loadBlockResult, this::mapping);
    }

    private Tuple<String, String> generateValue() {
        if (nullValue != null && ESTestCase.randomBoolean()) {
            return Tuple.tuple(null, nullValue);
        }
        int length = 5;
        if (ignoreAbove != null && (allIgnored || ESTestCase.randomBoolean())) {
            length = ignoreAbove + 5;
        }
        String v = ESTestCase.randomAlphaOfLength(length);
        return Tuple.tuple(v, v);
    }

    private void mapping(XContentBuilder b) throws IOException {
        b.field("type", "keyword");
        if (nullValue != null) {
            b.field("null_value", nullValue);
        }
        if (ignoreAbove != null) {
            b.field("ignore_above", ignoreAbove);
        }
        if (store) {
            b.field("store", true);
        }
        if (docValues == false) {
            b.field("doc_values", false);
        }
    }

    @Override
    public List<MapperTestCase.SyntheticSourceInvalidExample> invalidExample() throws IOException {
        return List.of();
    }
}
