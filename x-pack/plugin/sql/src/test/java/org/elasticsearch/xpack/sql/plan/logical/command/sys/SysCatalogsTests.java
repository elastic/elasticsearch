/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.analysis.analyzer.Verifier;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Configuration;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.stats.Metrics;
import org.elasticsearch.xpack.sql.type.TypesTests;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysCatalogsTests extends ESTestCase {

    private final SqlParser parser = new SqlParser();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private Tuple<Command, SqlSession> sql(String sql) {
        EsIndex test = new EsIndex("test", TypesTests.loadMapping("mapping-multi-field-with-nested.json", true));
        Analyzer analyzer = new Analyzer(Configuration.DEFAULT, new FunctionRegistry(), IndexResolution.valid(test),
                                         new Verifier(new Metrics()));
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql), true);

        IndexResolver resolver = mock(IndexResolver.class);
        when(resolver.clusterName()).thenReturn("cluster");

        doAnswer(invocation -> {
            ((ActionListener) invocation.getArguments()[2]).onResponse(singletonList(test));
            return Void.TYPE;
        }).when(resolver).resolveAsSeparateMappings(any(), any(), any());

        SqlSession session = new SqlSession(null, null, null, resolver, null, null, null, null);
        return new Tuple<>(cmd, session);
    }

    public void testSysCatalogs() throws Exception {
        Tuple<Command, SqlSession> sql = sql("SYS CATALOGS");

        sql.v1().execute(sql.v2(), ActionListener.wrap(r -> {
            assertEquals(1, r.size());
            assertEquals("cluster", r.column(0));
        }, ex -> fail(ex.getMessage())));
    }
}