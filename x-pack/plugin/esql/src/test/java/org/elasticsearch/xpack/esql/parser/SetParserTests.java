/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.Row;

import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class SetParserTests extends AbstractStatementParserTests {

    public void testSet() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = query("SET foo = \"bar\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar"));

        query = query("SET bar = 2; row a = 1 | eval x = 12", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Eval.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "bar", 2);

        query = query("SET bar = true; row a = 1 | eval x = 12", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Eval.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "bar", true);

        expectThrows(ParsingException.class, () -> query("SET foo = 1, bar = 2; row a = 1", new QueryParams()));
    }

    public void testSetWithTripleQuotes() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = query("SET foo = \"\"\"bar\"baz\"\"\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar\"baz"));

        query = query("SET foo = \"\"\"bar\"\"\"\"; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("bar\""));

        query = query("SET foo = \"\"\"\"bar\"\"\"; row a = 1 | LIMIT 3", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Limit.class)));
        assertThat(query.settings().size(), is(1));
        checkSetting(query, 0, "foo", BytesRefs.toBytesRef("\"bar"));
    }

    public void testMultipleSet() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = query(
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
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = query("SET foo = [\"bar\", \"baz\"]; SET bar = [1, 2, 3]; row a = 1", new QueryParams());
        assertThat(query.plan(), is(instanceOf(Row.class)));
        assertThat(query.settings().size(), is(2));

        checkSetting(query, 0, "foo", List.of(BytesRefs.toBytesRef("bar"), BytesRefs.toBytesRef("baz")));
        checkSetting(query, 1, "bar", List.of(1, 2, 3));
    }

    public void testSetWithNamedParams() {
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = query(
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
        assumeTrue("SET command available in snapshot only", EsqlCapabilities.Cap.SET_COMMAND.isEnabled());
        EsqlStatement query = query(
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

}
