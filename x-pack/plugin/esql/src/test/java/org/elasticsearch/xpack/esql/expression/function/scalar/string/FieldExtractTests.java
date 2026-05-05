/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link FieldExtract}.
 */
public class FieldExtractTests extends AbstractScalarFunctionTestCase {
    public FieldExtractTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType pathType : List.of(DataType.KEYWORD, DataType.TEXT)) {
            suppliers.add(
                new TestCaseSupplier(
                    "nested host name " + pathType.typeName(),
                    types(DataType.FLATTENED, pathType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("{\"host\":{\"name\":\"node-a\"}}"), DataType.FLATTENED, "field"),
                            new TestCaseSupplier.TypedData(new BytesRef("host.name"), pathType, "path")
                        ),
                        "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                        DataType.KEYWORD,
                        equalTo(new BytesRef("node-a"))
                    )
                )
            );
        }

        suppliers.add(
            new TestCaseSupplier(
                "constant path keyword",
                types(DataType.FLATTENED, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"k\":{\"inner\":\"v\"}}"), DataType.FLATTENED, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef("k.inner"), DataType.KEYWORD, "path").forceLiteral()
                    ),
                    "FieldExtractConstantEvaluator[flattenedJson=Attribute[channel=0], path=k.inner]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef("v"))
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "bracket key with dot",
                types(DataType.FLATTENED, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"service.name\":\"otel\"}"), DataType.FLATTENED, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef("['service.name']"), DataType.KEYWORD, "path")
                    ),
                    "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    equalTo(new BytesRef("otel"))
                )
            )
        );

        suppliers.add(
            new TestCaseSupplier(
                "missing path",
                types(DataType.FLATTENED, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("{\"a\":1}"), DataType.FLATTENED, "field"),
                        new TestCaseSupplier.TypedData(new BytesRef("missing"), DataType.KEYWORD, "path")
                    ),
                    "FieldExtractEvaluator[flattenedJson=Attribute[channel=0], path=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    nullValue()
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.IllegalArgumentException: path [missing] does not exist")
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new FieldExtract(source, args.get(0), args.get(1));
    }

    public void testConstantPathProducesConstantEvaluator() {
        Source source = Source.synthetic("field_extract");
        var evaluatorFactory = evaluator(
            new FieldExtract(source, field("field", DataType.FLATTENED), new Literal(source, new BytesRef("host.name"), DataType.KEYWORD))
        );
        assertThat(evaluatorFactory, instanceOf(FieldExtractConstantEvaluator.Factory.class));
    }

    public void testNonConstantPathProducesGenericEvaluator() {
        Source source = Source.synthetic("field_extract");
        var evaluatorFactory = evaluator(new FieldExtract(source, field("field", DataType.FLATTENED), field("path", DataType.KEYWORD)));
        assertThat(evaluatorFactory, instanceOf(FieldExtractEvaluator.Factory.class));
    }

    public void testFoldablePathWithArrayIndexFailsVerification() {
        var extract = new FieldExtract(
            Source.EMPTY,
            field("field", DataType.FLATTENED),
            new Literal(Source.EMPTY, new BytesRef("tags[0]"), DataType.KEYWORD)
        );
        assertTrue(extract.typeResolved().unresolved());
        assertThat(extract.typeResolved().message(), equalTo("field_extract path cannot use array indices"));
    }

    public void testFoldableEmptyPathFailsVerification() {
        var extract = new FieldExtract(
            Source.EMPTY,
            field("field", DataType.FLATTENED),
            new Literal(Source.EMPTY, new BytesRef(""), DataType.KEYWORD)
        );
        assertTrue(extract.typeResolved().unresolved());
        assertThat(extract.typeResolved().message(), equalTo("field_extract path must contain at least one key segment"));
    }

    private String extractFromBytes(BytesRef bytes, String path) {
        try (
            var eval = evaluator(new FieldExtract(Source.EMPTY, field("field", DataType.FLATTENED), field("path", DataType.KEYWORD))).get(
                driverContext()
            );
            Block block = eval.eval(row(List.of(bytes, new BytesRef(path))))
        ) {
            return block.isNull(0) ? null : ((BytesRef) BlockUtils.toJavaObject(block, 0)).utf8ToString();
        }
    }

    public void testSimpleNestedExtraction() {
        assertThat(extractFromBytes(new BytesRef("{\"a\":{\"b\":\"leaf\"}}"), "a.b"), equalTo("leaf"));
    }

    private static List<DataType> types(DataType firstType, DataType secondType) {
        return List.of(firstType, secondType);
    }
}
