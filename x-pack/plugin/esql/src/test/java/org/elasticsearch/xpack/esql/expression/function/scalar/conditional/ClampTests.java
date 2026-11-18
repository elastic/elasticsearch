/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.versionfield.Version;
import org.hamcrest.Matchers;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ClampTests extends AbstractScalarFunctionTestCase {
    private static final List<DataType> STRING_TYPES = List.of(DataType.KEYWORD, DataType.TEXT);
    private static final List<DataType> NUMERIC_TYPES = List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE);

    public ClampTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType fieldType : STRING_TYPES) {
            for (DataType minType : STRING_TYPES) {
                for (DataType maxType : STRING_TYPES) {
                    suppliers.add(
                        comparableCase(
                            fieldType,
                            minType,
                            maxType,
                            DataType.KEYWORD,
                            () -> new BytesRef(randomAlphaOfLength(3)),
                            BytesRef::compareTo
                        )
                    );
                }
            }
        }
        for (DataType fieldType : NUMERIC_TYPES) {
            for (DataType minType : NUMERIC_TYPES) {
                for (DataType maxType : NUMERIC_TYPES) {
                    suppliers.add(numericCase(fieldType, minType, maxType));
                }
            }
        }
        suppliers.add(sameTypeCase(DataType.UNSIGNED_LONG, ESTestCase::randomBigInteger, BigInteger::compareTo));
        suppliers.add(sameTypeCase(DataType.BOOLEAN, () -> randomBoolean(), Boolean::compare));
        suppliers.add(
            sameTypeCase(
                DataType.VERSION,
                () -> new Version(randomIntBetween(0, 20) + "." + randomIntBetween(0, 20) + "." + randomIntBetween(0, 20)).toBytesRef(),
                BytesRef::compareTo
            )
        );
        suppliers.add(
            sameTypeCase(DataType.IP, () -> new BytesRef(InetAddressPoint.encode(randomIp(randomBoolean()))), BytesRef::compareTo)
        );
        suppliers.add(sameTypeCase(DataType.DATETIME, () -> randomMillisUpToYear9999(), Long::compare));
        suppliers.add(sameTypeCase(DataType.DATE_NANOS, () -> randomLongBetween(0, Long.MAX_VALUE), Long::compare));
        // A null-typed field is rejected, see https://github.com/elastic/elasticsearch/issues/142537
        List<TestCaseSupplier> withNulls = anyNullIsNull(true, suppliers).stream()
            .filter(s -> s.types().getFirst() != DataType.NULL)
            .toList();
        return parameterSuppliersFromTypedData(withNulls);
    }

    private static <T> TestCaseSupplier sameTypeCase(DataType type, Supplier<T> randomValue, Comparator<T> comparator) {
        return comparableCase(type, type, type, type, randomValue, comparator);
    }

    /**
     * A case whose arguments share an evaluator type, so the expected value is computed with {@code comparator}.
     */
    private static <T> TestCaseSupplier comparableCase(
        DataType fieldType,
        DataType minType,
        DataType maxType,
        DataType resultType,
        Supplier<T> randomValue,
        Comparator<T> comparator
    ) {
        String evaluatorType = PlannerUtils.toElementType(resultType).pascalCaseName();
        return new TestCaseSupplier(List.of(fieldType, minType, maxType), () -> {
            T field = randomValue.get();
            T min = randomValue.get();
            T max = randomValue.get();
            T clampedToMin = comparator.compare(field, min) < 0 ? min : field;
            T expected = comparator.compare(clampedToMin, max) > 0 ? max : clampedToMin;
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, fieldType, "field"),
                    new TestCaseSupplier.TypedData(min, minType, "min"),
                    new TestCaseSupplier.TypedData(max, maxType, "max")
                ),
                "ClampMax"
                    + evaluatorType
                    + "Evaluator[field=ClampMin"
                    + evaluatorType
                    + "Evaluator[field=Attribute[channel=0], min=Attribute[channel=1]], max=Attribute[channel=2]]",
                resultType,
                equalTo(expected)
            );
        });
    }

    private static TestCaseSupplier numericCase(DataType fieldType, DataType minType, DataType maxType) {
        DataType clampedToMinType = widerNumericType(fieldType, minType);
        DataType resultType = widerNumericType(clampedToMinType, maxType);
        return new TestCaseSupplier(List.of(fieldType, minType, maxType), () -> {
            Number field = randomNumber(fieldType);
            Number min = randomNumber(minType);
            Number max = randomNumber(maxType);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(field, fieldType, "field"),
                    new TestCaseSupplier.TypedData(min, minType, "min"),
                    new TestCaseSupplier.TypedData(max, maxType, "max")
                ),
                Matchers.stringContainsInOrder(
                    "ClampMax",
                    PlannerUtils.toElementType(resultType).pascalCaseName(),
                    "Evaluator[field=",
                    "ClampMin",
                    PlannerUtils.toElementType(clampedToMinType).pascalCaseName(),
                    "Evaluator[field="
                ),
                resultType,
                equalTo(expectedNumber(resultType, field, min, max))
            );
        });
    }

    private static DataType widerNumericType(DataType a, DataType b) {
        if (a == DataType.DOUBLE || b == DataType.DOUBLE) {
            return DataType.DOUBLE;
        }
        if (a == DataType.LONG || b == DataType.LONG) {
            return DataType.LONG;
        }
        return DataType.INTEGER;
    }

    private static Number randomNumber(DataType type) {
        return switch (type) {
            case INTEGER -> randomIntBetween(-1000, 1000);
            case LONG -> randomBoolean() ? randomLongBetween(-1000, 1000) : randomLong();
            case DOUBLE -> randomDoubleBetween(-1000, 1000, true);
            default -> throw new IllegalArgumentException("unsupported type [" + type + "]");
        };
    }

    private static Number expectedNumber(DataType resultType, Number field, Number min, Number max) {
        return switch (resultType) {
            case INTEGER -> Math.min(Math.max(field.intValue(), min.intValue()), max.intValue());
            case LONG -> Math.min(Math.max(field.longValue(), min.longValue()), max.longValue());
            case DOUBLE -> Math.min(Math.max(field.doubleValue(), min.doubleValue()), max.doubleValue());
            default -> throw new IllegalArgumentException("unsupported result type [" + resultType + "]");
        };
    }

    @Override
    public void testFold() {}

    @Override
    protected EsqlScalarFunction build(Source source, List<Expression> args) {
        return new Clamp(source, args.get(0), args.get(1), args.get(2));
    }
}
