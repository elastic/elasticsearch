/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.multivalue;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.util.NumericUtils.unsignedLongToDouble;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MvAvgTests extends AbstractMultivalueFunctionTestCase {

    private static final Map<DataType, Function<Object, Double>> CONVERTER_MAP = Map.of(
        DataTypes.DOUBLE,
        x -> (Double) x,
        DataTypes.INTEGER,
        x -> ((Integer) x).doubleValue(),
        DataTypes.LONG,
        x -> ((Long) x).doubleValue(),
        DataTypes.UNSIGNED_LONG,
        x -> unsignedLongToDouble((Long) x)
    );

    public MvAvgTests(@Name("TestCase") Supplier<TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("mv_avg(<double>)", () -> {
            List<Double> mvData = randomList(1, 100, () -> randomDouble());
            return new TestCase(
                List.of(new TypedData(mvData, DataTypes.DOUBLE, "field")),
                "MvAvg[field=Attribute[channel=0]]",
                DataTypes.DOUBLE,
                equalTo(mvData.stream().mapToDouble(Double::doubleValue).summaryStatistics().getAverage())
            );
        })));
    }

    @Override
    protected Expression build(Source source, Expression field) {
        return new MvAvg(source, field);
    }

    @Override
    protected DataType[] supportedTypes() {
        return representableNumerics();
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.DOUBLE;  // Averages are always a double
    }

    @Override
    protected Matcher<Object> resultMatcherForInput(List<?> input, DataType dataType) {
        if (dataType == DataTypes.NULL) {
            return nullValue();
        }
        Function<Object, Double> converter = CONVERTER_MAP.get(dataType);
        if (converter == null) {
            throw new UnsupportedOperationException("unsupported type " + input);
        }
        CompensatedSum sum = new CompensatedSum();
        input.forEach(x -> sum.add(converter.apply(x)));
        return equalTo(sum.value() / input.size());
    }
}
