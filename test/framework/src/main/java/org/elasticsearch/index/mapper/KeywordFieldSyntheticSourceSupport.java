/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class KeywordFieldSyntheticSourceSupport implements MapperTestCase.SyntheticSourceSupport {
    private final Integer ignoreAbove;
    private final boolean allIgnored;
    private final boolean store;
    private final boolean docValues;
    private final String nullValue;
    private final boolean exampleSortsUsingIgnoreAbove;

    KeywordFieldSyntheticSourceSupport(Integer ignoreAbove, boolean store, String nullValue, boolean exampleSortsUsingIgnoreAbove) {
        this.ignoreAbove = ignoreAbove;
        this.allIgnored = ignoreAbove != null && LuceneTestCase.rarely();
        this.store = store;
        this.nullValue = nullValue;
        this.exampleSortsUsingIgnoreAbove = exampleSortsUsingIgnoreAbove;
        this.docValues = store ? ESTestCase.randomBoolean() : true;
    }

    @Override
    public MapperTestCase.SyntheticSourceExample example(int maxValues) {
        return example(maxValues, false);
    }

    public MapperTestCase.SyntheticSourceExample example(int maxValues, boolean loadBlockFromSource) {
        if (ESTestCase.randomBoolean()) {
            Tuple<String, String> v = generateValue();
            Object loadBlock = v.v2();
            if (loadBlockFromSource == false && ignoreAbove != null && v.v2().length() > ignoreAbove) {
                loadBlock = null;
            }
            return new MapperTestCase.SyntheticSourceExample(v.v1(), v.v2(), loadBlock, this::mapping);
        }
        List<Tuple<String, String>> values = ESTestCase.randomList(1, maxValues, this::generateValue);
        List<String> in = values.stream().map(Tuple::v1).toList();
        List<String> outPrimary = new ArrayList<>();
        List<String> outExtraValues = new ArrayList<>();
        values.stream().map(Tuple::v2).forEach(v -> {
            if (exampleSortsUsingIgnoreAbove && ignoreAbove != null && v.length() > ignoreAbove) {
                outExtraValues.add(v);
            } else {
                outPrimary.add(v);
            }
        });
        List<String> outList = store ? outPrimary : new HashSet<>(outPrimary).stream().sorted().collect(Collectors.toList());
        List<String> loadBlock;
        if (loadBlockFromSource) {
            // The block loader infrastructure will never return nulls. Just zap them all.
            loadBlock = in.stream().filter(m -> m != null).toList();
        } else if (docValues) {
            loadBlock = new HashSet<>(outPrimary).stream().sorted().collect(Collectors.toList());
        } else {
            loadBlock = List.copyOf(outList);
        }
        Object loadBlockResult = loadBlock.size() == 1 ? loadBlock.get(0) : loadBlock;
        outList.addAll(outExtraValues);
        Object out = outList.size() == 1 ? outList.get(0) : outList;
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
        return List.of(
            new MapperTestCase.SyntheticSourceInvalidExample(
                equalTo(
                    "field [field] of type [keyword] doesn't support synthetic source because "
                        + "it doesn't have doc values and isn't stored"
                ),
                b -> b.field("type", "keyword").field("doc_values", false)
            ),
            new MapperTestCase.SyntheticSourceInvalidExample(
                equalTo("field [field] of type [keyword] doesn't support synthetic source because it declares a normalizer"),
                b -> b.field("type", "keyword").field("normalizer", "lowercase")
            )
        );
    }
}
