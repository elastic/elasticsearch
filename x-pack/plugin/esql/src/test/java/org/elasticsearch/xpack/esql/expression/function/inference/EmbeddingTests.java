/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.inference;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;

import static org.elasticsearch.xpack.esql.core.type.DataType.DENSE_VECTOR;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.equalTo;

@FunctionName("embedding")
public class EmbeddingTests extends AbstractFunctionTestCase {
    public EmbeddingTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // Two-argument case (no options)
        suppliers.add(
            new TestCaseSupplier(
                List.of(KEYWORD, KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), KEYWORD, "text"),
                        new TestCaseSupplier.TypedData(randomBytesReference(10).toBytesRef(), KEYWORD, "inference_id")
                    ),
                    Matchers.blankOrNullString(),
                    DENSE_VECTOR,
                    equalTo(true)
                )
            )
        );

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        MapExpression options = args.size() > 2 ? (MapExpression) args.get(2) : null;
        return new Embedding(source, args.get(0), args.get(1), options);
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    public void testFoldableWithNoOptions() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("hello world"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        Embedding embedding = new Embedding(source, text, inferenceId, null);
        assertTrue(embedding.foldable());
        assertNull(embedding.inputOptions());
    }

    public void testFoldableWithOptions() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("image data"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        MapExpression options = new MapExpression(
            source,
            List.of(
                new Literal(source, new BytesRef("type"), DataType.KEYWORD),
                new Literal(source, new BytesRef("image"), DataType.KEYWORD)
            )
        );
        Embedding embedding = new Embedding(source, text, inferenceId, options);
        assertTrue(embedding.foldable());
        assertNotNull(embedding.inputOptions());
    }

    public void testToStringWithNoOptions() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("hello"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        Embedding embedding = new Embedding(source, text, inferenceId, null);
        assertThat(embedding.toString(), Matchers.startsWith("EMBEDDING("));
        assertThat(embedding.toString(), Matchers.not(Matchers.containsString("null")));
    }

    public void testReplaceChildrenWithNoOptions() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("hello"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        Embedding embedding = new Embedding(source, text, inferenceId, null);

        Literal newText = new Literal(source, new BytesRef("world"), DataType.KEYWORD);
        Literal newInferenceId = new Literal(source, new BytesRef("other-endpoint"), DataType.KEYWORD);
        Expression replaced = embedding.replaceChildren(List.of(newText, newInferenceId));
        assertThat(replaced, Matchers.instanceOf(Embedding.class));
        Embedding replacedEmbedding = (Embedding) replaced;
        assertThat(replacedEmbedding.inputText(), equalTo(newText));
        assertThat(replacedEmbedding.inferenceId(), equalTo(newInferenceId));
        assertNull(replacedEmbedding.inputOptions());
    }

    public void testDataTypeDefaultsToText() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("hello"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        Embedding embedding = new Embedding(source, text, inferenceId, null);
        assertEquals(org.elasticsearch.inference.DataType.TEXT, embedding.inputDataType());
    }

    public void testDataFormatDefaultsToText() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("hello"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        Embedding embedding = new Embedding(source, text, inferenceId, null);
        assertEquals(org.elasticsearch.inference.DataFormat.TEXT, embedding.inputDataFormat());
    }

    public void testDataTypeFromOptions() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("data"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        MapExpression options = new MapExpression(
            source,
            List.of(
                new Literal(source, new BytesRef("type"), DataType.KEYWORD),
                new Literal(source, new BytesRef("image"), DataType.KEYWORD)
            )
        );
        Embedding embedding = new Embedding(source, text, inferenceId, options);
        assertFalse(embedding.resolveType().unresolved());
        assertEquals(org.elasticsearch.inference.DataType.IMAGE, embedding.inputDataType());
    }

    public void testDataFormatFromOptions() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("data"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        MapExpression options = new MapExpression(
            source,
            List.of(
                new Literal(source, new BytesRef("type"), DataType.KEYWORD),
                new Literal(source, new BytesRef("image"), DataType.KEYWORD),
                new Literal(source, new BytesRef("format"), DataType.KEYWORD),
                new Literal(source, new BytesRef("base64"), DataType.KEYWORD)
            )
        );
        Embedding embedding = new Embedding(source, text, inferenceId, options);
        assertFalse(embedding.resolveType().unresolved());
        assertEquals(org.elasticsearch.inference.DataFormat.BASE64, embedding.inputDataFormat());
    }

    public void testResolveTypeRejectsUnknownOptionKey() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("data"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        MapExpression options = new MapExpression(
            source,
            List.of(
                new Literal(source, new BytesRef("unknown"), DataType.KEYWORD),
                new Literal(source, new BytesRef("foo"), DataType.KEYWORD)
            )
        );
        Embedding embedding = new Embedding(source, text, inferenceId, options);
        Expression.TypeResolution resolution = embedding.resolveType();
        assertTrue(resolution.unresolved());
        assertThat(resolution.message(), containsString("unknown"));
    }

    public void testResolveTypeRejectsInvalidDataType() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("data"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        MapExpression options = new MapExpression(
            source,
            List.of(
                new Literal(source, new BytesRef("type"), DataType.KEYWORD),
                new Literal(source, new BytesRef("video"), DataType.KEYWORD)
            )
        );
        Embedding embedding = new Embedding(source, text, inferenceId, options);
        assertTrue(embedding.resolveType().unresolved());
    }

    public void testResolveTypeRejectsInvalidDataFormat() {
        Source source = Source.EMPTY;
        Literal text = new Literal(source, new BytesRef("data"), DataType.KEYWORD);
        Literal inferenceId = new Literal(source, new BytesRef("my-endpoint"), DataType.KEYWORD);
        MapExpression options = new MapExpression(
            source,
            List.of(
                new Literal(source, new BytesRef("format"), DataType.KEYWORD),
                new Literal(source, new BytesRef("xml"), DataType.KEYWORD)
            )
        );
        Embedding embedding = new Embedding(source, text, inferenceId, options);
        assertTrue(embedding.resolveType().unresolved());
    }
}
