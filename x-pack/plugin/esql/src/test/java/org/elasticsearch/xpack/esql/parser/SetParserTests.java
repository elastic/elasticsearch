/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomizeCase;
import static org.elasticsearch.xpack.esql.plan.QuerySettings.UNMAPPED_FIELDS;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SetParserTests extends AbstractStatementParserTests {

    public void testSet() {
        EsqlStatement query = unvalidatedStatement("SET foo = \"bar\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"));

        query = unvalidatedStatement("SET bar = 2; row a = 1 | eval x = 12", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Eval.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "bar", 2);

        query = unvalidatedStatement("SET bar = true; row a = 1 | eval x = 12", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Eval.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "bar", true);

        expectThrows(ParsingException.class, () -> statement("SET foo = 1, bar = 2; row a = 1", new QueryParams()));
    }

    public void testSetWithTripleQuotes() {
        EsqlStatement query = unvalidatedStatement("SET foo = \"\"\"bar\"baz\"\"\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar\"baz"));

        query = unvalidatedStatement("SET foo = \"\"\"bar\"\"\"\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar\""));

        query = unvalidatedStatement("SET foo = \"\"\"\"bar\"\"\"; row a = 1 | LIMIT 3", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Limit.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("\"bar"));
    }

    public void testMultipleSet() {
        EsqlStatement query = unvalidatedStatement(
            "SET foo = \"bar\"; SET bar = 2; SET foo = \"baz\"; SET x = 3.5; SET y = false; SET z = null; row a = 1",
            new QueryParams()
        );
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(6));

        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz"));
        checkSetting(query, 1, "bar", 2);
        checkSetting(query, 2, "foo", BytesRefs.toBytesRef("baz"));
        checkSetting(query, 3, "x", 3.5);
        checkSetting(query, 4, "y", false);
        checkSetting(query, 5, "z", null);
    }

    public void testSetArrays() {
        EsqlStatement query = unvalidatedStatement("SET foo = [\"bar\", \"baz\"]; SET bar = [1, 2, 3]; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(2));

        checkSetting(query, 0, "foo", List.of(BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz")));
        checkSetting(query, 1, "bar", List.of(1, 2, 3));
    }

    public void testSetWithNamedParams() {
        EsqlStatement query = unvalidatedStatement(
            "SET foo = \"bar\"; SET bar = ?a; SET foo = \"baz\"; SET x = ?x; row a = 1",
            new QueryParams(
                List.of(
                    new QueryParam("a", 2, DataType.INTEGER, ParserUtils.ParamClassification.VALUE),
                    new QueryParam("x", 3.5, DataType.DOUBLE, ParserUtils.ParamClassification.VALUE)
                )
            )
        );
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(4));

        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz"));
        checkSetting(query, 1, "bar", 2);
        checkSetting(query, 2, "foo", BytesRefs.toBytesRef("baz"));
        checkSetting(query, 3, "x", 3.5);
    }

    public void testSetWithPositionalParams() {
        EsqlStatement query = unvalidatedStatement(
            "SET foo = \"bar\"; SET bar = ?; SET foo = \"baz\"; SET x = ?; row a = ?",
            new QueryParams(
                List.of(
                    new QueryParam("a", 2, DataType.INTEGER, ParserUtils.ParamClassification.VALUE),
                    new QueryParam("x", 3.5, DataType.DOUBLE, ParserUtils.ParamClassification.VALUE),
                    new QueryParam("y", 8, DataType.DOUBLE, ParserUtils.ParamClassification.VALUE)
                )
            )
        );
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(((Row) query.plan()).fields().get(0).child().fold(FoldContext.small()), is(8));
        assertThat(query.settings().size(), is(4));

        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz"));
        checkSetting(query, 1, "bar", 2);
        checkSetting(query, 2, "foo", BytesRefs.toBytesRef("baz"));
        checkSetting(query, 3, "x", 3.5);
    }

    @SuppressWarnings("unchecked")
    public void testSetWithMap() {
        // non-constant map
        try {
            unvalidatedStatement("""
                SET my_map = {"foo": bar};
                ROW a = 1
                """, new QueryParams());
            fail("ParsingException expected");
        } catch (ParsingException e) {
            assertThat(e.getMessage(), containsString("mismatched input 'bar' expecting"));
        }

        EsqlStatement query = unvalidatedStatement("""
            SET my_map = {"foo": {"bar": 2, "baz": "bb"}, "x": false};
            ROW a = 1
            """, new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));

        assertThat(settingName(query, 0), is("my_map"));
        Object value = ((MapExpression) query.settings().get(0).value()).toFoldedMap(FoldContext.small());
        assertThat(value, instanceOf(Map.class));
        Map<String, Object> map = (Map<String, Object>) value;
        assertThat(map.size(), is(2));
        assertThat(map.get("x"), is(false));
        Object nested = map.get("foo");
        assertThat(nested, instanceOf(Map.class));
        Map<String, Object> nestedMap = (Map<String, Object>) nested;
        assertThat(nestedMap.size(), is(2));
        assertThat(nestedMap.get("bar"), is(2));
        assertThat(nestedMap.get("baz"), is(BytesRefs.toBytesRef("bb")));
    }

    /**
     * @param query    the query
     * @param position the order of the corresponding SET statement
     * @param name     the setting name
     * @param value    the setting value as it appears in the query at that position
     */
    private void checkSetting(EsqlStatement query, int position, String name, Object value) {
        checkSetting(query, position, name, value, value);
    }

    /**
     * @param query        the query
     * @param position     the order of the corresponding SET statement
     * @param name         the setting name
     * @param value        the setting value as it appears in the query at that position
     * @param maskingValue the final value you'll obtain if you use query.setting(name).
     *                     It could be different from value in case of name collisions in the query
     */
    private void checkSetting(EsqlStatement query, int position, String name, Object value, Object maskingValue) {
        assertThat(settingName(query, position), is(name));
        assertThat(settingValue(query, position), is(value));
        assertThat(query.setting(name).fold(FoldContext.small()), is(maskingValue));
    }

    private String settingName(EsqlStatement query, int position) {
        return query.settings().get(position).name();
    }

    private Object settingValue(EsqlStatement query, int position) {
        return query.settings().get(position).value().fold(FoldContext.small());
    }

    public void testSetUnmappedFields_snapshot() {
        assumeTrue("OPTIONAL_FIELDS option required", EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled());

        var modes = List.of("FAIL", "NULLIFY", "LOAD");
        verifySetUnmappedFields(modes);
        assertThat(modes.size(), is(UnmappedResolution.values().length));
    }

    public void testSetUnmappedFields_nonSnapshot() {
        assumeFalse("Requires no snapshot", Build.current().isSnapshot());

        verifySetUnmappedFields(List.of("FAIL", "NULLIFY"));

        String name = randomizeCase(UnmappedResolution.LOAD.name());
        expectThrows(
            ParsingException.class,
            containsString(
                "Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution ["
                    + name
                    + "], must be one of [FAIL, NULLIFY]"
            ),
            () -> statement("SET unmapped_fields=\"" + name + "\"; row a = 1")
        );
    }

    private void verifySetUnmappedFields(List<String> modes) {
        for (var mode : modes) {
            EsqlStatement statement = statement("SET unmapped_fields=\"" + randomizeCase(mode) + "\"; row a = 1");
            assertThat(statement.setting(UNMAPPED_FIELDS), is(UnmappedResolution.valueOf(mode)));
            assertThat(statement.plan(), is(instanceOf(Row.class)));
        }
    }

    public void testSetUnmappedFieldsWrongValue() {
        var mode = randomValueOtherThanMany(
            v -> Arrays.stream(UnmappedResolution.values()).anyMatch(x -> x.name().equalsIgnoreCase(v)),
            () -> randomAlphaOfLengthBetween(0, 10)
        );
        var values = EsqlCapabilities.Cap.OPTIONAL_FIELDS.isEnabled()
            ? UnmappedResolution.values()
            : Arrays.stream(UnmappedResolution.values()).filter(e -> e != UnmappedResolution.LOAD).toArray();
        expectValidationError(
            "SET unmapped_fields=\"" + mode + "\"; row a = 1",
            "Error validating setting [unmapped_fields]: Invalid unmapped_fields resolution ["
                + mode
                + "], must be one of "
                + Arrays.toString(values)
        );
    }
}
