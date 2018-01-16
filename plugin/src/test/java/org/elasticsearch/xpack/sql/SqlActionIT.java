/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.plugin.AbstractSqlRequest.Mode;
import org.elasticsearch.xpack.sql.plugin.ColumnInfo;
import org.elasticsearch.xpack.sql.plugin.MetaColumnInfo;
import org.elasticsearch.xpack.sql.plugin.SqlListColumnsAction;
import org.elasticsearch.xpack.sql.plugin.SqlListColumnsResponse;
import org.elasticsearch.xpack.sql.plugin.SqlListTablesAction;
import org.elasticsearch.xpack.sql.plugin.SqlListTablesResponse;
import org.elasticsearch.xpack.sql.plugin.SqlQueryAction;
import org.elasticsearch.xpack.sql.plugin.SqlQueryResponse;

import java.io.IOException;
import java.sql.JDBCType;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class SqlActionIT extends AbstractSqlIntegTestCase {
    public void testSqlAction() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").get());
        client().prepareBulk()
                .add(new IndexRequest("test", "doc", "1").source("data", "bar", "count", 42))
                .add(new IndexRequest("test", "doc", "2").source("data", "baz", "count", 43))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
        ensureYellow("test");

        boolean dataBeforeCount = randomBoolean();
        String columns = dataBeforeCount ? "data, count" : "count, data";
        SqlQueryResponse response = client().prepareExecute(SqlQueryAction.INSTANCE)
                .query("SELECT " + columns + " FROM test ORDER BY count").mode(Mode.JDBC).get();
        assertThat(response.size(), equalTo(2L));
        assertThat(response.columns(), hasSize(2));
        int dataIndex = dataBeforeCount ? 0 : 1;
        int countIndex = dataBeforeCount ? 1 : 0;
        assertEquals(new ColumnInfo("", "data", "text", JDBCType.VARCHAR, 0), response.columns().get(dataIndex));
        assertEquals(new ColumnInfo("", "count", "long", JDBCType.BIGINT, 20), response.columns().get(countIndex));

        assertThat(response.rows(), hasSize(2));
        assertEquals("bar", response.rows().get(0).get(dataIndex));
        assertEquals(42L, response.rows().get(0).get(countIndex));
        assertEquals("baz", response.rows().get(1).get(dataIndex));
        assertEquals(43L, response.rows().get(1).get(countIndex));
    }

    public void testSqlListTablesAction() throws Exception {

        createCompatibleIndex("foo");
        createCompatibleIndex("bar");
        createCompatibleIndex("baz");
        createIncompatibleIndex("broken");

        SqlListTablesResponse response = client().prepareExecute(SqlListTablesAction.INSTANCE)
                .pattern("").get();
        List<String> tables = removeInternal(response.getTables());
        assertThat(tables, hasSize(4));
        assertThat(tables, containsInAnyOrder("foo", "bar", "baz", "broken"));


        response = client().prepareExecute(SqlListTablesAction.INSTANCE).pattern("b%").get();
        tables = removeInternal(response.getTables());
        assertThat(tables, hasSize(3));
        assertThat(tables, containsInAnyOrder("bar", "baz", "broken"));

        response = client().prepareExecute(SqlListTablesAction.INSTANCE).pattern("not_found").get();
        tables = removeInternal(response.getTables());
        assertThat(tables, emptyCollectionOf(String.class));

        response = client().prepareExecute(SqlListTablesAction.INSTANCE).pattern("broken").get();
        tables = removeInternal(response.getTables());
        assertThat(tables, hasSize(1));
    }


    public void testSqlListColumnsAction() throws Exception {

        createCompatibleIndex("bar");
        createCompatibleIndex("baz");
        createIncompatibleIndex("broken");

        SqlListColumnsResponse response = client().prepareExecute(SqlListColumnsAction.INSTANCE)
                .indexPattern("bar").columnPattern("").mode(Mode.JDBC).get();
        List<MetaColumnInfo> columns = response.getColumns();
        assertThat(columns, hasSize(2));
        assertThat(columns, contains(
                new MetaColumnInfo("bar", "int_field", "integer", JDBCType.INTEGER, 10, 1),
                new MetaColumnInfo("bar", "str_field", "text", JDBCType.VARCHAR, Integer.MAX_VALUE, 2)
        ));

        response = client().prepareExecute(SqlListColumnsAction.INSTANCE)
                .indexPattern("bar").columnPattern("").mode(Mode.PLAIN).get();
        columns = response.getColumns();
        assertThat(columns, hasSize(2));
        assertThat(columns, contains(
                new MetaColumnInfo("bar", "int_field", "integer", null, 0, 1),
                new MetaColumnInfo("bar", "str_field", "text", null, 0, 2)
        ));
    }

    private void createCompatibleIndex(String name) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject();
        {
            mapping.startObject("properties");
            {
                mapping.startObject("str_field").field("type", "text").endObject();
                mapping.startObject("int_field").field("type", "integer").endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();

        assertAcked(client().admin().indices().prepareCreate(name).addMapping("doc", mapping).get());
    }

    private void createIncompatibleIndex(String name) throws IOException {
        assertAcked(client().admin().indices().prepareCreate(name).get());
    }

    /**
     * Removes list of internal indices to make tests consistent between secure and unsecure environments
     */
    private static List<String> removeInternal(List<String> list) {
        return list.stream().filter(s -> s.startsWith(".") == false).collect(Collectors.toList());
    }
}

