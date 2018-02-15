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
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolution;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver.IndexInfo;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver.IndexType;
import org.elasticsearch.xpack.sql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.sql.parser.ParsingException;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.type.TypesTests;
import org.joda.time.DateTimeZone;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.elasticsearch.action.ActionListener.wrap;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysTablesTests extends ESTestCase {

    private final SqlParser parser = new SqlParser(DateTimeZone.UTC);
    private final Map<String, EsField> mapping = TypesTests.loadMapping("mapping-multi-field-with-nested.json", true);
    private final IndexInfo index = new IndexInfo("test", IndexType.INDEX);
    private final IndexInfo alias = new IndexInfo("alias", IndexType.ALIAS);

    public void testSysTablesDifferentCatalog() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE 'foo'", r -> {
            assertEquals(0, r.size());
            assertFalse(r.hasCurrentRow());
        });
    }

    public void testSysTablesNoTypes() throws Exception {
        executeCommand("SYS TABLES", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
        }, index, alias);
    }

    public void testSysTablesPattern() throws Exception {
        executeCommand("SYS TABLES LIKE '%'", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
        }, index, alias);
    }

    public void testSysTablesOnlyAliases() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'ALIAS'", r -> {
            assertEquals(1, r.size());
            assertEquals("alias", r.column(2));
        }, alias);
    }

    public void testSysTablesOnlyIndices() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'BASE TABLE'", r -> {
            assertEquals(1, r.size());
            assertEquals("test", r.column(2));
        }, index);
    }

    public void testSysTablesOnlyIndicesAndAliases() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'ALIAS', 'BASE TABLE'", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
        }, index, alias);
    }

    public void testSysTablesWithInvalidType() throws Exception {
        ParsingException pe = expectThrows(ParsingException.class, () -> sql("SYS TABLES LIKE 'test' TYPE 'QUE HORA ES'"));
        assertEquals("line 1:2: Invalid table type [QUE HORA ES]", pe.getMessage());
    }

    private Tuple<Command, SqlSession> sql(String sql) {
        EsIndex test = new EsIndex("test", mapping);
        Analyzer analyzer = new Analyzer(new FunctionRegistry(), IndexResolution.valid(test), DateTimeZone.UTC);
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql), true);

        IndexResolver resolver = mock(IndexResolver.class);
        when(resolver.clusterName()).thenReturn("cluster");

        SqlSession session = new SqlSession(null, null, null, resolver, null, null, null);
        return new Tuple<>(cmd, session);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void executeCommand(String sql, Consumer<SchemaRowSet> consumer, IndexInfo... infos) throws Exception {
        Tuple<Command, SqlSession> tuple = sql(sql);

        IndexResolver resolver = tuple.v2().indexResolver();

        doAnswer(invocation -> {
            ((ActionListener) invocation.getArguments()[3]).onResponse(new LinkedHashSet<>(asList(infos)));
            return Void.TYPE;
        }).when(resolver).resolveNames(any(), any(), any(), any());

        tuple.v1().execute(tuple.v2(), wrap(consumer::accept, ex -> fail(ex.getMessage())));
    }
}