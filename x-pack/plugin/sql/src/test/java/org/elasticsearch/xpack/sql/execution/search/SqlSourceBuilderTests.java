/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class SqlSourceBuilderTests extends ESTestCase {
    public void testSqlSourceBuilder() {
        final QlSourceBuilder ssb = new QlSourceBuilder();
        final SearchSourceBuilder source = new SearchSourceBuilder();
        ssb.trackScores();
        ssb.addSourceField("foo");
        ssb.addSourceField("foo2");
        ssb.addDocField("bar", null);
        ssb.addDocField("bar2", null);
        final Script s = new Script("eggplant");
        ssb.addScriptField("baz", s);
        final Script s2 = new Script("potato");
        ssb.addScriptField("baz2", s2);
        ssb.build(source);

        assertTrue(source.trackScores());
        FetchSourceContext fsc = source.fetchSource();
        assertThat(Arrays.asList(fsc.includes()), contains("foo", "foo2"));
        assertThat(source.docValueFields().stream().map(ff -> ff.field).collect(Collectors.toList()), contains("bar", "bar2"));
        Map<String, Script> scriptFields = source.scriptFields()
                .stream()
                .collect(Collectors.toMap(SearchSourceBuilder.ScriptField::fieldName, SearchSourceBuilder.ScriptField::script));
        assertThat(scriptFields.get("baz").getIdOrCode(), equalTo("eggplant"));
        assertThat(scriptFields.get("baz2").getIdOrCode(), equalTo("potato"));
    }
}
