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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsBigInteger;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.dateTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.ipToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.nanoTimeToString;
import static org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter.versionToString;
import static org.hamcrest.Matchers.equalTo;

public class JsonStringTests extends AbstractScalarFunctionTestCase {

    private static final List<DataType> VALUE_TYPES = List.of(
        DataType.BOOLEAN,
        DataType.INTEGER,
        DataType.LONG,
        DataType.UNSIGNED_LONG,
        DataType.DOUBLE,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.IP,
        DataType.VERSION,
        DataType.DATETIME,
        DataType.DATE_NANOS
    );

    public JsonStringTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        // A single key/value pair for every combination of key type and value type. This also covers the
        // supported-types matrix required by the function info checks.
        for (DataType keyType : List.of(DataType.KEYWORD, DataType.TEXT)) {
            for (DataType valueType : VALUE_TYPES) {
                suppliers.add(singlePair(keyType, valueType));
            }
        }
        suppliers.add(twoPairs());
        suppliers.add(escaping());
        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(suppliers));
    }

    private static TestCaseSupplier singlePair(DataType keyType, DataType valueType) {
        return new TestCaseSupplier(keyType.typeName() + ", " + valueType.typeName(), List.of(keyType, valueType), () -> {
            BytesRef key = new BytesRef(randomAlphaOfLengthBetween(1, 10));
            Object value = randomLiteral(valueType).value();
            String json = "{" + quote(key.utf8ToString()) + ":" + fragment(valueType, value) + "}";
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(key, keyType, "key0"),
                new TestCaseSupplier.TypedData(value, valueType, "value0")
            );
            return new TestCaseSupplier.TestCase(data, expectedToString(1), DataType.KEYWORD, equalTo(new BytesRef(json)));
        });
    }

    private static TestCaseSupplier twoPairs() {
        return new TestCaseSupplier("two pairs", List.of(DataType.KEYWORD, DataType.INTEGER, DataType.KEYWORD, DataType.KEYWORD), () -> {
            BytesRef k0 = new BytesRef(randomAlphaOfLengthBetween(1, 8));
            Integer v0 = randomInt();
            BytesRef k1 = new BytesRef(randomAlphaOfLengthBetween(1, 8));
            BytesRef v1 = new BytesRef(randomAlphaOfLengthBetween(1, 8));
            String json = "{" + quote(k0.utf8ToString()) + ":" + v0 + "," + quote(k1.utf8ToString()) + ":" + quote(v1.utf8ToString()) + "}";
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(k0, DataType.KEYWORD, "key0"),
                new TestCaseSupplier.TypedData(v0, DataType.INTEGER, "value0"),
                new TestCaseSupplier.TypedData(k1, DataType.KEYWORD, "key1"),
                new TestCaseSupplier.TypedData(v1, DataType.KEYWORD, "value1")
            );
            return new TestCaseSupplier.TestCase(data, expectedToString(2), DataType.KEYWORD, equalTo(new BytesRef(json)));
        });
    }

    private static TestCaseSupplier escaping() {
        return new TestCaseSupplier("escaping", List.of(DataType.KEYWORD, DataType.KEYWORD), () -> {
            // key: a"b\c value: line1<newline>line2<tab>"q"
            BytesRef key = new BytesRef("a\"b\\c");
            BytesRef value = new BytesRef("line1\nline2\t\"q\"");
            String json = "{\"a\\\"b\\\\c\":\"line1\\nline2\\t\\\"q\\\"\"}";
            List<TestCaseSupplier.TypedData> data = List.of(
                new TestCaseSupplier.TypedData(key, DataType.KEYWORD, "key0"),
                new TestCaseSupplier.TypedData(value, DataType.KEYWORD, "value0")
            );
            return new TestCaseSupplier.TestCase(data, expectedToString(1), DataType.KEYWORD, equalTo(new BytesRef(json)));
        });
    }

    private static String fragment(DataType valueType, Object value) {
        return switch (valueType) {
            case BOOLEAN, INTEGER, LONG, DOUBLE -> String.valueOf(value);
            case UNSIGNED_LONG -> unsignedLongAsBigInteger((Long) value).toString();
            case KEYWORD, TEXT -> quote(((BytesRef) value).utf8ToString());
            case IP -> quote(ipToString((BytesRef) value));
            case VERSION -> quote(versionToString((BytesRef) value));
            case DATETIME -> quote(dateTimeToString((Long) value));
            case DATE_NANOS -> quote(nanoTimeToString((Long) value));
            default -> throw new IllegalArgumentException("unsupported value type [" + valueType + "]");
        };
    }

    /**
     * Quotes a string that is known not to contain any JSON special characters. Escaping is validated
     * separately in {@link #escaping()}.
     */
    private static String quote(String value) {
        return "\"" + value + "\"";
    }

    private static String expectedToString(int pairs) {
        StringBuilder keys = new StringBuilder("[");
        StringBuilder values = new StringBuilder("[");
        for (int i = 0; i < pairs; i++) {
            if (i > 0) {
                keys.append(", ");
                values.append(", ");
            }
            keys.append("Attribute[channel=").append(i * 2).append("]");
            values.append("Attribute[channel=").append(i * 2 + 1).append("]");
        }
        keys.append("]");
        values.append("]");
        return "JsonStringEvaluator[keys=" + keys + ", values=" + values + "]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new JsonString(source, args);
    }
}
