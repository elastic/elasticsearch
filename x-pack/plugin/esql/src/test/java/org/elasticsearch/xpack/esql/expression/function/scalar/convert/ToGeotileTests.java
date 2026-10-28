/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.longToGeotile;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.stringToGeotile;

@FunctionName("to_geotile")
public class ToGeotileTests extends AbstractScalarFunctionTestCase {
    public ToGeotileTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String attribute = "Attribute[channel=0]";
        final String evaluator = "ToGeotileFromStringEvaluator[in=Attribute[channel=0]]";
        final String fromLong = "ToGeotileFromLongEvaluator[in=Attribute[channel=0]]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryGeoGrid(suppliers, attribute, DataType.GEOTILE, DataType.GEOTILE, v -> v, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, fromLong, DataType.LONG, DataType.GEOTILE, v -> v, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, evaluator, DataType.KEYWORD, DataType.GEOTILE, ToGeotileTests::valueOf, List.of());
        TestCaseSupplier.forUnaryGeoGrid(suppliers, evaluator, DataType.TEXT, DataType.GEOTILE, ToGeotileTests::valueOf, List.of());

        // Invalid values produce a warning and null, instead of failing later when rendering the results
        TestCaseSupplier.forUnaryGeoGridInvalid(
            suppliers,
            fromLong,
            DataType.LONG,
            DataType.GEOTILE,
            List.of(1L, 50L, -1L, Long.MIN_VALUE, 30L << 58),
            v -> expectThrows(IllegalArgumentException.class, () -> longToGeotile((Long) v))
        );
        TestCaseSupplier.forUnaryGeoGridInvalid(
            suppliers,
            evaluator,
            DataType.KEYWORD,
            DataType.GEOTILE,
            Stream.of("0/0/1", "0/0/-1", "30/0/0", "1/2/0", "not a tile").<Object>map(BytesRef::new).toList(),
            v -> expectThrows(IllegalArgumentException.class, () -> stringToGeotile(((BytesRef) v).utf8ToString()))
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    private static long valueOf(Object gridAddress) {
        assert gridAddress instanceof BytesRef;
        return GeoTileUtils.longEncode(((BytesRef) gridAddress).utf8ToString());
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToGeotile(source, args.get(0));
    }
}
