/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link PromqlSetLabel}, the identity-blob write scalar shared by PromQL {@code label_replace} and
 * {@code label_join}. The namespace routing, {@code __name__} special-casing and canonical (sorted) output are asserted
 * directly on {@code rewrite}; the null(no-op)/empty(delete)/value(set) encoding is exercised through a real evaluator.
 */
public class PromqlSetLabelTests extends ESTestCase {

    private final List<CircuitBreaker> breakers = Collections.synchronizedList(new ArrayList<>());

    // --- Namespace routing, __name__ special-casing, canonical ordering ---

    public void testSetIntoPrometheusNamespace() throws IOException {
        assertThat(rewrite("""
            {"labels":{"a":"1"}}""", "b", "2"), equalTo("""
            {"labels":{"a":"1","b":"2"}}"""));
    }

    public void testDeleteFromPrometheusNamespace() throws IOException {
        assertThat(rewrite("""
            {"labels":{"a":"1","b":"2"}}""", "b", ""), equalTo("""
            {"labels":{"a":"1"}}"""));
    }

    public void testSetIntoOtelNamespace() throws IOException {
        assertThat(rewrite("""
            {"attributes":{"a":"1"}}""", "b", "2"), equalTo("""
            {"attributes":{"a":"1","b":"2"}}"""));
    }

    public void testNameLabelForPrometheusStaysInLabels() throws IOException {
        assertThat(rewrite("""
            {"labels":{"__name__":"m","a":"1"}}""", "__name__", "n"), equalTo("""
            {"labels":{"__name__":"n","a":"1"}}"""));
    }

    public void testNameLabelForOtelSurfacesBare() throws IOException {
        assertThat(rewrite("""
            {"attributes":{"a":"1"}}""", "__name__", "n"), equalTo("""
            {"__name__":"n","attributes":{"a":"1"}}"""));
    }

    public void testOutputIsCanonicallySorted() throws IOException {
        // Unsorted input must still produce sorted output.
        assertThat(rewrite("""
            {"labels":{"z":"1","a":"2"}}""", "m", "3"), equalTo("""
            {"labels":{"a":"2","m":"3","z":"1"}}"""));
    }

    public void testInsertSortsBeforeExistingEvenWhenInputSorted() throws IOException {
        // Input is already sorted, but inserting a key that sorts first still requires reordering the output.
        assertThat(rewrite("""
            {"labels":{"m":"1"}}""", "a", "2"), equalTo("""
            {"labels":{"a":"2","m":"1"}}"""));
    }

    public void testSetOverwritesExistingValue() throws IOException {
        assertThat(rewrite("""
            {"labels":{"a":"1"}}""", "a", "9"), equalTo("""
            {"labels":{"a":"9"}}"""));
    }

    public void testBothNamespacesRegularLabelGoesToLabels() throws IOException {
        // With both passthrough namespaces present, a regular label writes into labels; the root stays sorted.
        assertThat(rewrite("""
            {"attributes":{"x":"1"},"labels":{"a":"1"}}""", "b", "2"), equalTo("""
            {"attributes":{"x":"1"},"labels":{"a":"1","b":"2"}}"""));
    }

    public void testSetNameLabelIntoLabelsWhenAbsent() throws IOException {
        assertThat(rewrite("""
            {"labels":{"a":"1"}}""", "__name__", "n"), equalTo("""
            {"labels":{"__name__":"n","a":"1"}}"""));
    }

    public void testDeleteNameLabelFromLabels() throws IOException {
        assertThat(rewrite("""
            {"labels":{"__name__":"m","a":"1"}}""", "__name__", ""), equalTo("""
            {"labels":{"a":"1"}}"""));
    }

    public void testDeleteBareNameLabelForOtel() throws IOException {
        assertThat(rewrite("""
            {"__name__":"n","attributes":{"a":"1"}}""", "__name__", ""), equalTo("""
            {"attributes":{"a":"1"}}"""));
    }

    public void testDeleteRegularLabelFromAbsentNamespaceIsNoOp() throws IOException {
        // A blob carrying only a bare OTel __name__ has no passthrough namespace; deleting a regular label leaves it as is,
        // without materializing an empty namespace object.
        assertThat(rewrite("""
            {"__name__":"x"}""", "foo", ""), equalTo("""
            {"__name__":"x"}"""));
    }

    // --- Value fidelity: non-string types, arrays, nested objects, escaping and unicode ---

    public void testPreservesNonStringValuesFromSortedInput() throws IOException {
        // Untouched numeric/boolean values must be carried through with their JSON type, not re-encoded as strings, while the
        // new value is written as a string.
        assertThat(rewrite("""
            {"labels":{"a":1,"z":true}}""", "m", "2"), equalTo("""
            {"labels":{"a":1,"m":"2","z":true}}"""));
    }

    public void testPreservesNonStringValuesFromUnsortedInput() throws IOException {
        assertThat(rewrite("""
            {"labels":{"z":true,"a":1}}""", "m", "2"), equalTo("""
            {"labels":{"a":1,"m":"2","z":true}}"""));
    }

    public void testPreservesDecimalAndNullValues() throws IOException {
        assertThat(rewrite("""
            {"attributes":{"a":1.5,"z":null}}""", "m", "2"), equalTo("""
            {"attributes":{"a":1.5,"m":"2","z":null}}"""));
    }

    public void testPreservesArrayValueOrder() throws IOException {
        // Arrays are carried through verbatim; their element order must not be touched by key sorting.
        assertThat(rewrite("""
            {"attributes":{"a":[3,1,2]}}""", "m", "x"), equalTo("""
            {"attributes":{"a":[3,1,2],"m":"x"}}"""));
    }

    public void testSortsNestedObjectValue() throws IOException {
        // Object-valued labels are sorted recursively, preserving their leaf value types.
        assertThat(rewrite("""
            {"attributes":{"a":{"y":1,"x":2}}}""", "m", "x"), equalTo("""
            {"attributes":{"a":{"x":2,"y":1},"m":"x"}}"""));
    }

    public void testEscapesNewValue() throws IOException {
        // The written value is JSON-string encoded, so characters like a quote are escaped.
        assertThat(
            PromqlSetLabel.rewrite(new BytesRef("{\"labels\":{\"a\":\"1\"}}"), "m", new BytesRef("a\"b"), false).utf8ToString(),
            equalTo("{\"labels\":{\"a\":\"1\",\"m\":\"a\\\"b\"}}")
        );
    }

    public void testPreservesUnicodeValues() throws IOException {
        // Existing multi-byte values are carried through and the new value is written as UTF-8, unescaped.
        assertThat(rewrite("""
            {"labels":{"a":"naïve"}}""", "m", "café"), equalTo("""
            {"labels":{"a":"naïve","m":"café"}}"""));
    }

    public void testDeleteWhilePreservingSortedNeighbours() throws IOException {
        assertThat(rewrite("""
            {"labels":{"a":"1","m":"2","z":"3"}}""", "m", ""), equalTo("""
            {"labels":{"a":"1","z":"3"}}"""));
    }

    // --- Evaluator: null (no-op) preserves the blob byte-for-byte, non-null writes ---

    public void testEvaluatorNoOpPreservesBlobExactly() {
        // Unsorted input; a no-op must return it unchanged (it is not canonicalized).
        String blob = "{\"labels\":{\"z\":\"1\",\"a\":\"2\"}}";
        assertThat(eval(blob, null, "b"), equalTo(blob));
    }

    public void testEvaluatorSet() {
        assertThat(eval("{\"labels\":{\"a\":\"1\"}}", "2", "b"), equalTo("{\"labels\":{\"a\":\"1\",\"b\":\"2\"}}"));
    }

    public void testEvaluatorDelete() {
        assertThat(eval("{\"labels\":{\"a\":\"1\",\"b\":\"2\"}}", "", "b"), equalTo("{\"labels\":{\"a\":\"1\"}}"));
    }

    private static String rewrite(String blob, String name, String value) throws IOException {
        return PromqlSetLabel.rewrite(new BytesRef(blob), name, new BytesRef(value), value.isEmpty()).utf8ToString();
    }

    private String eval(String blob, String value, String dstName) {
        Source source = Source.EMPTY;
        PromqlSetLabel function = new PromqlSetLabel(
            source,
            field("timeseries", DataType.KEYWORD),
            field("value", DataType.KEYWORD),
            new Literal(source, new BytesRef(dstName), DataType.KEYWORD)
        );
        BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();
        try (
            BytesRefBlock.Builder timeseries = blockFactory.newBytesRefBlockBuilder(1);
            BytesRefBlock.Builder valueBuilder = blockFactory.newBytesRefBlockBuilder(1)
        ) {
            timeseries.appendBytesRef(new BytesRef(blob));
            if (value == null) {
                valueBuilder.appendNull();
            } else {
                valueBuilder.appendBytesRef(new BytesRef(value));
            }
            try (
                var evaluator = AbstractScalarFunctionTestCase.evaluator(function).get(driverContext());
                Block block = evaluator.eval(new Page(timeseries.build(), valueBuilder.build()))
            ) {
                return ((BytesRefBlock) block).getBytesRef(0, new BytesRef()).utf8ToString();
            }
        }
    }

    private static FieldAttribute field(String name, DataType type) {
        return new FieldAttribute(Source.synthetic(name), name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private DriverContext driverContext() {
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(256)).withCircuitBreaking();
        breakers.add(bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST));
        return new DriverContext(bigArrays, BlockFactory.builder(bigArrays).build(), null);
    }

    @After
    public void allBreakersEmpty() {
        for (CircuitBreaker breaker : breakers) {
            assertThat("Breaker not empty: " + breaker.getName(), breaker.getUsed(), equalTo(0L));
        }
    }
}
