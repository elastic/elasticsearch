/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.jdbc;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SysColumnsTestCase extends JdbcIntegrationTestCase {

    public void testAliasWithIncompatibleTypes() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("value").field("type", "double").endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "text").endObject();
            builder.startObject("value").field("type", "double").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "test1", "id", "KEYWORD" },
                { "test1", "value", "DOUBLE" },
                { "test2", "id", "TEXT" },
                { "test2", "value", "DOUBLE" },
                { "test_alias", "value", "DOUBLE" } }
        );
    }

    public void testAliasWithIncompatibleSearchableProperty() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").field("index", false).endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createIndexWithMapping("test3", builder -> {
            builder.startObject("id").field("type", "keyword").field("index", false).endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createIndexWithMapping("test4", builder -> {
            builder.startObject("id").field("type", "keyword").field("index", false).endObject();
            builder.startObject("value").field("type", "boolean").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test3").field("alias", "test_alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test4").field("alias", "test_alias2").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "test1", "id", "KEYWORD" },
                { "test1", "value", "BOOLEAN" },
                { "test2", "id", "KEYWORD" },
                { "test2", "value", "BOOLEAN" },
                { "test3", "id", "KEYWORD" },
                { "test3", "value", "BOOLEAN" },
                { "test4", "id", "KEYWORD" },
                { "test4", "value", "BOOLEAN" },
                { "test_alias", "value", "BOOLEAN" },
                { "test_alias2", "id", "KEYWORD" },
                { "test_alias2", "value", "BOOLEAN" } }
        );
    }

    public void testAliasWithIncompatibleAggregatableProperty() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "text").field("fielddata", true).endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "text").endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createIndexWithMapping("test3", builder -> {
            builder.startObject("id").field("type", "text").field("fielddata", true).endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createIndexWithMapping("test4", builder -> {
            builder.startObject("id").field("type", "text").field("fielddata", true).endObject();
            builder.startObject("value").field("type", "date").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test3").field("alias", "test_alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test4").field("alias", "test_alias2").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "test1", "id", "TEXT" },
                { "test1", "value", "DATETIME" },
                { "test2", "id", "TEXT" },
                { "test2", "value", "DATETIME" },
                { "test3", "id", "TEXT" },
                { "test3", "value", "DATETIME" },
                { "test4", "id", "TEXT" },
                { "test4", "value", "DATETIME" },
                { "test_alias", "value", "DATETIME" },
                { "test_alias2", "id", "TEXT" },
                { "test_alias2", "value", "DATETIME" }, }
        );
    }

    public void testAliasWithIncompatibleTypesInSubfield() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "test1", "id", "TEXT" },
                { "test1", "id.raw", "KEYWORD" },
                { "test1", "value", "DATETIME" },
                { "test1", "value.raw", "LONG" },
                { "test2", "id", "TEXT" },
                { "test2", "id.raw", "INTEGER" },
                { "test2", "value", "DATETIME" },
                { "test2", "value.raw", "LONG" },
                { "test_alias", "id", "TEXT" },
                { "test_alias", "value", "DATETIME" },
                { "test_alias", "value.raw", "LONG" }, }
        );
    }

    public void testAliasWithIncompatibleSearchablePropertyInSubfield() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .field("index", false)
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "date")
                .startObject("fields")
                .startObject("raw")
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "test1", "id", "TEXT" },
                { "test1", "id.raw", "INTEGER" },
                { "test1", "value", "DATETIME" },
                { "test1", "value.raw", "LONG" },
                { "test2", "id", "TEXT" },
                { "test2", "id.raw", "INTEGER" },
                { "test2", "value", "DATETIME" },
                { "test2", "value.raw", "LONG" },
                { "test_alias", "id", "TEXT" },
                { "test_alias", "value", "DATETIME" },
                { "test_alias", "value.raw", "LONG" }, }
        );
    }

    public void testAliasWithIncompatibleAggregatablePropertyInSubfield() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "ip")
                .startObject("fields")
                .startObject("raw")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject();
            builder.startObject("value")
                .field("type", "ip")
                .startObject("fields")
                .startObject("raw")
                .field("type", "text")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "test1", "id", "TEXT" },
                { "test1", "id.raw", "INTEGER" },
                { "test1", "value", "IP" },
                { "test1", "value.raw", "TEXT" },
                { "test2", "id", "TEXT" },
                { "test2", "id.raw", "INTEGER" },
                { "test2", "value", "IP" },
                { "test2", "value.raw", "TEXT" },
                { "test_alias", "id", "TEXT" },
                { "test_alias", "value", "IP" },
                { "test_alias", "value.raw", "TEXT" }, }
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/53445")
    public void testAliasWithSubfieldsAndDifferentRootFields() throws Exception {
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "text")
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
        });

        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name")
                .field("type", "keyword")
                .field("index", false)
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "test_alias").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "test_alias").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "test1", "id", "KEYWORD" },
                { "test1", "name", "TEXT" },
                { "test1", "name.raw", "KEYWORD" },
                { "test2", "id", "KEYWORD" },
                { "test2", "name", "KEYWORD" },
                { "test2", "name.raw", "KEYWORD" },
                { "test_alias", "id", "KEYWORD" } }
        );
    }

    public void testMultiIndicesMultiAlias() throws Exception {
        createIndexWithMapping("test2", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "text").endObject();
        });
        createIndexWithMapping("test4", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "text").field("index", false).endObject();
        });
        createIndexWithMapping("test1", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "keyword").endObject();
            builder.startObject("number").field("type", "long").endObject();
        });
        createIndexWithMapping("test3", builder -> {
            builder.startObject("id").field("type", "keyword").endObject();
            builder.startObject("name").field("type", "keyword").endObject();
            builder.startObject("number").field("type", "long").endObject();
        });

        createAliases(builder -> {
            builder.startObject().startObject("add").field("index", "test1").field("alias", "alias1").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test1").field("alias", "alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "alias2").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test2").field("alias", "alias3").endObject().endObject();
            builder.startObject().startObject("add").field("index", "test4").field("alias", "alias3").endObject().endObject();
        });

        assertResultsForQuery(
            "SYS COLUMNS",
            new String[][] {
                { "alias1", "id", "KEYWORD" },
                { "alias1", "name", "KEYWORD" },
                { "alias1", "number", "LONG" },
                { "alias2", "id", "KEYWORD" },
                { "alias2", "number", "LONG" },
                { "alias3", "id", "KEYWORD" },
                { "test1", "id", "KEYWORD" },
                { "test1", "name", "KEYWORD" },
                { "test1", "number", "LONG" },
                { "test2", "id", "KEYWORD" },
                { "test2", "name", "TEXT" },
                { "test3", "id", "KEYWORD" },
                { "test3", "name", "KEYWORD" },
                { "test3", "number", "LONG" },
                { "test4", "id", "KEYWORD" },
                { "test4", "name", "TEXT" } }
        );
    }

    private static void createIndexWithMapping(String indexName, CheckedConsumer<XContentBuilder, IOException> mapping) throws Exception {
        createIndex(indexName);
        updateMapping(indexName, mapping);
    }

    private void doWithQuery(String query, CheckedConsumer<ResultSet, SQLException> consumer) throws SQLException {
        try (Connection connection = esJdbc()) {
            try (PreparedStatement statement = connection.prepareStatement(query)) {
                try (ResultSet results = statement.executeQuery()) {
                    consumer.accept(results);
                }
            }
        }
    }

    private static void createAliases(CheckedConsumer<XContentBuilder, IOException> definitions) throws Exception {
        Request request = new Request("POST", "/_aliases");
        XContentBuilder createAliases = JsonXContent.contentBuilder().startObject();
        createAliases.startArray("actions");
        {
            definitions.accept(createAliases);
        }
        createAliases.endArray();
        createAliases.endObject();
        request.setJsonEntity(Strings.toString(createAliases));
        client().performRequest(request);
    }

    private void assertResultsForQuery(String query, String[][] rows) throws Exception {
        doWithQuery(query, (results) -> {
            for (String[] row : rows) {
                results.next();
                assertEquals(row[0], results.getString(3)); // table name
                assertEquals(row[1], results.getString(4)); // column name
                assertEquals(row[2], results.getString(6)); // type name
            }
            assertFalse(results.next());
        });
    }
}
