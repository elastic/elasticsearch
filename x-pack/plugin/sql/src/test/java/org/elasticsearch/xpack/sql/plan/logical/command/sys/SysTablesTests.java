/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ql.index.EsIndex;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.index.IndexResolver.IndexInfo;
import org.elasticsearch.xpack.ql.index.IndexResolver.IndexType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.type.EsField;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.action.Protocol;
import org.elasticsearch.xpack.sql.analysis.analyzer.Analyzer;
import org.elasticsearch.xpack.sql.parser.SqlParser;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlTypedParamValue;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlConfiguration;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.types.SqlTypesTests;
import org.elasticsearch.xpack.sql.util.DateUtils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.ql.index.IndexResolver.SQL_TABLE;
import static org.elasticsearch.xpack.ql.index.IndexResolver.SQL_VIEW;
import static org.elasticsearch.xpack.sql.analysis.analyzer.AnalyzerTestUtils.analyzer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SysTablesTests extends ESTestCase {

    private static final String CLUSTER_NAME = "cluster";

    private final SqlParser parser = new SqlParser();
    private final Map<String, EsField> mapping = SqlTypesTests.loadMapping("mapping-multi-field-with-nested.json", true);
    private final IndexInfo index = new IndexInfo(CLUSTER_NAME, "test", IndexType.STANDARD_INDEX);
    private final IndexInfo alias = new IndexInfo(CLUSTER_NAME, "alias", IndexType.ALIAS);
    private final IndexInfo frozen = new IndexInfo(CLUSTER_NAME, "frozen", IndexType.FROZEN_INDEX);

    private final SqlConfiguration FROZEN_CFG = new SqlConfiguration(
        DateUtils.UTC,
        null,
        Protocol.FETCH_SIZE,
        Protocol.REQUEST_TIMEOUT,
        Protocol.PAGE_TIMEOUT,
        null,
        null,
        Mode.PLAIN,
        null,
        null,
        null,
        null,
        false,
        true,
        null,
        null,
        false
    );

    //
    // catalog enumeration
    //
    public void testSysTablesCatalogEnumerationWithEmptyType() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '%' LIKE '' TYPE ''", r -> {
            assertEquals(1, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            // everything else should be null
            for (int i = 1; i < 10; i++) {
                assertNull(r.column(i));
            }
        }, index);
    }

    public void testSysTablesCatalogAllTypes() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '%' LIKE '' TYPE '%'", r -> {
            assertEquals(1, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            // everything else should be null
            for (int i = 1; i < 10; i++) {
                assertNull(r.column(i));
            }
        }, new IndexInfo[0]);
    }

    // when types are null, consider them equivalent to '' for compatibility reasons
    public void testSysTablesCatalogNoTypes() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '%' LIKE ''", r -> {
            assertEquals(1, r.size());
            assertEquals(CLUSTER_NAME, r.column(0));
            // everything else should be null
            for (int i = 1; i < 10; i++) {
                assertNull(r.column(i));
            }
        }, index);
    }

    //
    // table types enumeration
    //

    // missing type means pattern
    public void testSysTablesTypesEnumerationWoString() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '' LIKE '' ", r -> {
            assertEquals(2, r.size());
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals(SQL_VIEW, r.column(3));
        }, alias, index);
    }

    public void testSysTablesTypesEnumeration() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '' LIKE '' TYPE '%'", r -> {
            assertEquals(2, r.size());

            Iterator<IndexType> it = IndexType.VALID_REGULAR.stream().sorted(Comparator.comparing(IndexType::toSql)).iterator();

            for (int t = 0; t < r.size(); t++) {
                assertEquals(it.next().toSql(), r.column(3));

                // everything else should be null
                for (int i = 0; i < 10; i++) {
                    if (i != 3) {
                        assertNull(r.column(i));
                    }
                }

                r.advanceRow();
            }
        }, new IndexInfo[0]);
    }

    // when a type is specified, apply filtering
    public void testSysTablesTypesEnumerationAllCatalogsAndSpecifiedView() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '%' LIKE '' TYPE 'VIEW'", r -> { assertEquals(0, r.size()); }, new IndexInfo[0]);
    }

    public void testSysTablesDifferentCatalog() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE 'foo'", r -> {
            assertEquals(0, r.size());
            assertFalse(r.hasCurrentRow());
        });
    }

    public void testSysTablesLocalCatalog() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '" + CLUSTER_NAME + "'", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
            assertEquals(SQL_VIEW, r.column(3));
        }, index, alias);
    }

    public void testSysTablesNoTypes() throws Exception {
        executeCommand("SYS TABLES", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
            assertEquals(SQL_VIEW, r.column(3));
        }, index, alias);
    }

    public void testSysTablesNoTypesAndFrozen() throws Exception {
        executeCommand("SYS TABLES", r -> {
            assertEquals(3, r.size());
            assertEquals("frozen", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
            assertEquals(SQL_VIEW, r.column(3));
        }, FROZEN_CFG, index, alias, frozen);
    }

    public void testSysTablesWithTypes() throws Exception {
        executeCommand("SYS TABLES TYPE 'TABLE', 'ALIAS'", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
            assertEquals(SQL_VIEW, r.column(3));
        }, index, alias);
    }

    public void testSysTablesWithTypesAndFrozen() throws Exception {
        executeCommand("SYS TABLES TYPE 'TABLE', 'ALIAS'", r -> {
            assertEquals(3, r.size());
            assertEquals("frozen", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
            assertEquals(SQL_VIEW, r.column(3));
        }, index, frozen, alias);
    }

    public void testSysTablesPattern() throws Exception {
        executeCommand("SYS TABLES LIKE '%'", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
        }, index, alias);
    }

    public void testSysTablesPatternParameterized() throws Exception {
        List<SqlTypedParamValue> params = asList(param("%"));
        executeCommand("SYS TABLES LIKE ?", params, r -> {
            assertEquals("test", r.column(2));
            assertTrue(r.advanceRow());
            assertEquals(2, r.size());
            assertEquals("alias", r.column(2));
        }, alias, index);
    }

    public void testSysTablesOnlyAliases() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'ALIAS'", r -> {
            assertEquals(1, r.size());
            assertEquals("alias", r.column(2));
        }, alias);
    }

    public void testSysTablesOnlyAliasesParameterized() throws Exception {
        List<SqlTypedParamValue> params = asList(param("ALIAS"));
        executeCommand("SYS TABLES LIKE 'test' TYPE ?", params, r -> {
            assertEquals(1, r.size());
            assertEquals("alias", r.column(2));
        }, alias);
    }

    public void testSysTablesOnlyIndices() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'TABLE'", r -> {
            assertEquals(1, r.size());
            assertEquals("test", r.column(2));
        }, index);
    }

    public void testSysTablesOnlyIndicesWithFrozen() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'TABLE'", r -> {
            assertEquals(2, r.size());
            assertEquals("frozen", r.column(2));
            assertTrue(r.advanceRow());
            assertEquals("test", r.column(2));
        }, index, frozen);
    }

    public void testSysTablesNoPatternWithTypesSpecified() throws Exception {
        executeCommand("SYS TABLES TYPE 'TABLE','VIEW'", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
            assertEquals(SQL_VIEW, r.column(3));

        }, index, alias);
    }

    public void testSysTablesOnlyIndicesParameterizedTable() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE ?", asList(param("TABLE")), r -> {
            assertEquals(1, r.size());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
        }, index);
    }

    public void testSysTablesOnlyIndicesParameterizedAlias() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE ?", asList(param("ALIAS")), r -> {
            assertEquals(1, r.size());
            assertEquals("test", r.column(2));
        }, index);
    }

    public void testSysTablesOnlyIndicesAndAliases() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'VIEW', 'TABLE'", r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
        }, index, alias);
    }

    public void testSysTablesOnlyIndicesAndAliasesParameterized() throws Exception {
        List<SqlTypedParamValue> params = asList(param("VIEW"), param("TABLE"));
        executeCommand("SYS TABLES LIKE 'test' TYPE ?, ?", params, r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
        }, index, alias);
    }

    public void testSysTablesOnlyIndicesLegacyAndAliasesParameterized() throws Exception {
        List<SqlTypedParamValue> params = asList(param("VIEW"), param("TABLE"));
        executeCommand("SYS TABLES LIKE 'test' TYPE ?, ?", params, r -> {
            assertEquals(2, r.size());
            assertEquals("test", r.column(2));
            assertEquals(SQL_TABLE, r.column(3));
            assertTrue(r.advanceRow());
            assertEquals("alias", r.column(2));
            assertEquals(SQL_VIEW, r.column(3));
        }, index, alias);
    }

    public void testSysTablesWithCatalogOnlyAliases() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE '%' LIKE 'test' TYPE 'VIEW'", r -> {
            assertEquals(1, r.size());
            assertEquals("alias", r.column(2));
        }, alias);
    }

    public void testSysTablesWithCatalogPatternOnlyAliases() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE 'clus*' LIKE '%' TYPE 'VIEW'", r -> {
            assertEquals(1, r.size());
            assertEquals("alias", r.column(2));
        }, alias);
    }

    public void testSysTablesWithNoCatalogOnlyAliases() throws Exception {
        executeCommand("SYS TABLES TYPE 'VIEW'", r -> {
            assertEquals(1, r.size());
            assertEquals("alias", r.column(2));
        }, alias);
    }

    public void testSysTablesWithNonExistentCatalogOnlyAliases() throws Exception {
        executeCommand("SYS TABLES CATALOG LIKE 'bogus' LIKE '%' TYPE 'VIEW'", r -> { assertEquals(0, r.size()); });
    }

    public void testSysTablesWithInvalidType() throws Exception {
        executeCommand("SYS TABLES LIKE 'test' TYPE 'QUE HORA ES'", r -> { assertEquals(0, r.size()); }, new IndexInfo[0]);
    }

    private SqlTypedParamValue param(Object value) {
        return new SqlTypedParamValue(DataTypes.fromJava(value).typeName(), value);
    }

    private Tuple<Command, SqlSession> sql(String sql, List<SqlTypedParamValue> params, SqlConfiguration cfg) {
        EsIndex test = new EsIndex("test", mapping);
        Analyzer analyzer = analyzer(IndexResolution.valid(test));
        Command cmd = (Command) analyzer.analyze(parser.createStatement(sql, params, cfg.zoneId()), true);

        IndexResolver resolver = mock(IndexResolver.class);
        when(resolver.clusterName()).thenReturn(CLUSTER_NAME);

        SqlSession session = new SqlSession(cfg, null, null, resolver, null, null, null, null, null);
        return new Tuple<>(cmd, session);
    }

    private void executeCommand(String sql, Consumer<SchemaRowSet> consumer, IndexInfo... infos) throws Exception {
        executeCommand(sql, emptyList(), consumer, infos);
    }

    private void executeCommand(String sql, Consumer<SchemaRowSet> consumer, SqlConfiguration cfg, IndexInfo... infos) throws Exception {
        executeCommand(sql, emptyList(), consumer, cfg, infos);
    }

    private void executeCommand(String sql, List<SqlTypedParamValue> params, Consumer<SchemaRowSet> consumer, IndexInfo... infos)
        throws Exception {
        executeCommand(sql, params, consumer, SqlTestUtils.TEST_CFG, infos);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void executeCommand(
        String sql,
        List<SqlTypedParamValue> params,
        Consumer<SchemaRowSet> consumer,
        SqlConfiguration cfg,
        IndexInfo... infos
    ) throws Exception {
        Tuple<Command, SqlSession> tuple = sql(sql, params, cfg);

        IndexResolver resolver = tuple.v2().indexResolver();

        doAnswer(invocation -> {
            ((ActionListener) invocation.getArguments()[4]).onResponse(new LinkedHashSet<>(asList(infos)));
            return Void.TYPE;
        }).when(resolver).resolveNames(any(), any(), any(), any(), any());

        tuple.v1().execute(tuple.v2(), ActionTestUtils.assertNoFailureListener(p -> consumer.accept((SchemaRowSet) p.rowSet())));
    }
}
