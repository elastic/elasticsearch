/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SqlSourceBuilderTests extends ESTestCase {

    public void testSqlSourceBuilder() {
        final QlSourceBuilder ssb = new QlSourceBuilder();
        final SearchSourceBuilder source = new SearchSourceBuilder();
        ssb.trackScores();
        ssb.addFetchField("foo", null);
        ssb.addFetchField("foo2", "test");
        final Script s = new Script("eggplant");
        ssb.addScriptField("baz", s);
        final Script s2 = new Script("potato");
        ssb.addScriptField("baz2", s2);
        ssb.build(source);

        assertTrue(source.trackScores());
        assertNull(source.fetchSource());
        assertNull(source.docValueFields());

        List<FieldAndFormat> fetchFields = source.fetchFields();
        assertThat(fetchFields.size(), equalTo(2));
        assertThat(fetchFields.get(0).field, equalTo("foo"));
        assertThat(fetchFields.get(0).format, is(nullValue()));
        assertThat(fetchFields.get(1).field, equalTo("foo2"));
        assertThat(fetchFields.get(1).format, equalTo("test"));

        Map<String, Script> scriptFields = source.scriptFields()
                .stream()
                .collect(Collectors.toMap(SearchSourceBuilder.ScriptField::fieldName, SearchSourceBuilder.ScriptField::script));
        assertThat(scriptFields.get("baz").getIdOrCode(), equalTo("eggplant"));
        assertThat(scriptFields.get("baz2").getIdOrCode(), equalTo("potato"));
    }
}
