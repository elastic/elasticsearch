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
import java.util.stream.Stream;

public class KeywordFieldSyntheticSourceSupport implements MapperTestCase.SyntheticSourceSupport {
    private final Integer ignoreAbove;
    private final boolean allIgnored;
    private final boolean store;
    private final FieldMapper.DocValuesParameter.Values docValues;
    private final String nullValue;
    private final boolean allowIgnoredSource;
    private final boolean isColumnar;

    KeywordFieldSyntheticSourceSupport(
        Integer ignoreAbove,
        boolean store,
        String nullValue,
        boolean allowIgnoredSource,
        FieldMapper.DocValuesParameter.Values docValues,
        boolean isColumnar
    ) {
        this.ignoreAbove = ignoreAbove;
        this.allIgnored = ignoreAbove != null && LuceneTestCase.rarely();
        this.store = store;
        this.nullValue = nullValue;
        this.allowIgnoredSource = allowIgnoredSource;
        this.docValues = docValues;
        this.isColumnar = isColumnar;
    }

    @Override
    public boolean isColumnar() {
        return isColumnar;
    }

    public static FieldMapper.DocValuesParameter.Values randomDocValuesParams(boolean allowIgnoredSource, boolean isColumnar) {
        // multi_value=false is only valid in strict-columnar index modes.
        boolean multiValue = isColumnar == false || ESTestCase.randomBoolean();
        FieldMapper.DocValuesParameter.Values.OnFailure onFailure = ESTestCase.randomFrom(
            FieldMapper.DocValuesParameter.Values.OnFailure.FAIL,
            FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE
        );

        // Generate nullability=true only: nullability=false has no synthetic-source roundtrip behavior to fuzz.
        return switch (ESTestCase.randomInt(allowIgnoredSource ? 2 : 1)) {
            case 0 -> new FieldMapper.DocValuesParameter.Values(
                true,
                FieldMapper.DocValuesParameter.Values.Cardinality.LOW,
                multiValue,
                true,
                onFailure
            );
            case 1 -> new FieldMapper.DocValuesParameter.Values(
                true,
                FieldMapper.DocValuesParameter.Values.Cardinality.HIGH,
                multiValue,
                true,
                onFailure
            );
            case 2 -> FieldMapper.DocValuesParameter.Values.DISABLED_LOW_CARDINALITY;
            default -> throw new IllegalStateException();
        };
    }

    @Override
    public boolean ignoreAbove() {
        return ignoreAbove != null;
    }

    @Override
    public boolean enforcesSingleValue() {
        // multi_value=false with on_failure=FAIL throws on extra values → force single-valued examples only.
        // multi_value=false with on_failure=IGNORE redirects extra values to ._on_failure → multi-valued examples are valid.
        return docValues.multiValue() == false && docValues.onFailure() == FieldMapper.DocValuesParameter.Values.OnFailure.FAIL;
    }

    /**
     * {@code true} when extra values are silently redirected to the {@code ._on_failure} sidecar column rather than causing a parse error.
     * The example generator uses this to produce multi-valued inputs whose expected reconstruction equals the original array (encounter
     * order preserved; primary column carries the first value, sidecar carries the rest).
     */
    public boolean redirectsMultipleValues() {
        return docValues.multiValue() == false && docValues.onFailure() == FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE;
    }

    @Override
    public boolean preservesExactSource() {
        // We opt in into fallback synthetic source implementation
        // if there is nothing else to use, and it preserves exact source data.
        return store == false && docValues.enabled() == false;
    }

    @Override
    public MapperTestCase.SyntheticSourceExample example(int maxValues) {
        // in columnar mode, ignored values (exceeding ignore_above) are stored in sorted binary doc values
        return example(maxValues, false, false, isColumnar);
    }

    public MapperTestCase.SyntheticSourceExample example(int maxValues, boolean loadBlockFromSource, boolean flipOrder) {
        return example(maxValues, loadBlockFromSource, flipOrder, true);
    }

    public MapperTestCase.SyntheticSourceExample example(
        int maxValues,
        boolean loadBlockFromSource,
        boolean flipOrder,
        boolean ignoredValuesSorted
    ) {
        // When multi_value is disabled and on_failure=FAIL a document may only have a single value.
        // When multi_value=false and on_failure=IGNORE, extra values are redirected to ._on_failure and the full array is reconstructed.
        if (enforcesSingleValue() || (redirectsMultipleValues() == false && ESTestCase.randomBoolean())) {
            Tuple<String, String> v = generateValue();
            Object sourceValue = preservesExactSource() ? v.v1() : v.v2();
            return new MapperTestCase.SyntheticSourceExample(v.v1(), sourceValue, this::mapping);
        }

        if (redirectsMultipleValues()) {
            // Generate 2+ values; first goes to the primary column, rest to ._on_failure. Encounter order is preserved end-to-end.
            List<Tuple<String, String>> values = ESTestCase.randomList(2, maxValues, this::generateValue);
            List<String> in = values.stream().map(Tuple::v1).toList();
            // Non-null values only: leading nulls consume the single-value slot and their array position is lost (accepted lossiness).
            List<String> nonNullIn = in.stream().filter(v -> v != null).toList();
            Object out = nonNullIn.size() == 1 ? nonNullIn.get(0) : nonNullIn;
            return new MapperTestCase.SyntheticSourceExample(in, out, this::mapping);
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
        // columnar mode preserves insertion order and duplicates; non-columnar deduplicates and sorts
        List<String> outputFromDocValues = isColumnar ? validValues : new HashSet<>(validValues).stream().sorted().toList();

        Object out;
        if (preservesExactSource()) {
            out = in;
        } else {
            // stored fields are not sorted
            var validValuesInCorrectOrder = store ? validValues : outputFromDocValues;
            // when fallback fields use binary doc values, then ignored values are sorted
            // however, when fallback fields use stored fields, then ignored values are not sorted
            var ignoredValuesInCorrectOrder = ignoredValuesSorted ? ignoredValues.stream().sorted().toList() : ignoredValues;

            // this is an ugly little hack that flips the order of ignored values, which is important for the text-family fields where the
            // ordering of produced synthetic source values can be different from what was supplied
            var syntheticSourceOutputList = flipOrder
                ? Stream.concat(ignoredValuesInCorrectOrder.stream(), validValuesInCorrectOrder.stream()).toList()
                : Stream.concat(validValuesInCorrectOrder.stream(), ignoredValuesInCorrectOrder.stream()).toList();
            out = syntheticSourceOutputList.size() == 1 ? syntheticSourceOutputList.get(0) : syntheticSourceOutputList;
        }

        return new MapperTestCase.SyntheticSourceExample(in, out, this::mapping);
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

        if (docValues.enabled() == false) {
            b.field("doc_values", false);
        } else if (docValues.multiValue() == false) {
            b.startObject("doc_values");
            b.field("multi_value", false);
            if (docValues.onFailure() == FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE) {
                b.field("on_failure", "ignore");
            }
            b.endObject();
        } else {
            b.field("doc_values", true);
        }
    }

    @Override
    public List<MapperTestCase.SyntheticSourceInvalidExample> invalidExample() throws IOException {
        return List.of();
    }
}
