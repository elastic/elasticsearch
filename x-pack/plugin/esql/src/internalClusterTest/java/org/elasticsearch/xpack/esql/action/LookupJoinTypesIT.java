/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = SUITE, numClientNodes = 1, numDataNodes = 1)
public class LookupJoinTypesIT extends ESIntegTestCase {

    private static final Map<DataType, DataType> compatibleJoinTypes = Map.of(
        DataType.KEYWORD,
        DataType.KEYWORD,
        DataType.TEXT,
        DataType.KEYWORD,
        DataType.INTEGER,
        DataType.INTEGER,
        DataType.FLOAT,
        DataType.FLOAT,
        DataType.DOUBLE,
        DataType.DOUBLE
    );

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(EsqlPlugin.class);
    }

    public void testLookupJoinTypes() {
        initIndexes();
        initData();
        for (Map.Entry<DataType, DataType> entry : compatibleJoinTypes.entrySet()) {
            String query = String.format(
                Locale.ROOT,
                "FROM index | LOOKUP JOIN %s ON field_%s | KEEP other",
                indexName(entry.getKey(), entry.getValue()),
                entry.getKey().esType()
            );
            try (var response = EsqlQueryRequestBuilder.newRequestBuilder(client()).query(query).get()) {
                Iterator<Object> results = response.response().column(0).iterator();
                assertTrue("Expected at least one result for query: " + query, results.hasNext());
                Object indexedResult = response.response().column(0).iterator().next();
                assertThat("Expected valid result: " + query, indexedResult, equalTo("value"));
            }
        }
    }

    private void initIndexes() {
        // The main index will have many fields, one of each type to use in later type specific joins
        StringBuilder mainFields = new StringBuilder("{\n  \"properties\" : {\n");
        mainFields.append(
            compatibleJoinTypes.keySet()
                .stream()
                .map((l) -> "\"field_" + l.esType() + "\": { \"type\" : \"" + l.esType() + "\" }")
                .collect(Collectors.joining(",\n    "))
        );
        mainFields.append("  }\n}\n");
        assertAcked(prepareCreate("index").setMapping(mainFields.toString()));

        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.mode", "lookup");
        compatibleJoinTypes.forEach(
            // Each lookup index will get a document with a field to join on, and a results field to get back
            (l, r) -> { assertAcked(prepareCreate(indexName(l, r)).setSettings(settings.build()).setMapping(String.format(Locale.ROOT, """
                {
                  "properties" : {
                   "field_%s": { "type" : "%s" },
                   "other": { "type" : "keyword" }
                  }
                }
                """, l.esType(), r.esType()))); }
        );
    }

    private String indexName(DataType mainType, DataType lookupType) {
        return "index_" + mainType.esType() + "_" + lookupType.esType();
    }

    private void initData() {
        List<String> mainProperties = new ArrayList<>();
        int docId = 0;
        for (Map.Entry<DataType, DataType> entry : compatibleJoinTypes.entrySet()) {
            DataType mainType = entry.getKey();
            DataType lookupType = entry.getValue();
            String index = indexName(mainType, lookupType);
            String field = "field_" + mainType.esType();
            String value = sampleDataFor(lookupType);
            String doc = String.format(Locale.ROOT, """
                {
                  "%s": %s,
                  "other": "value"
                }
                """, field, value);
            mainProperties.add(String.format(Locale.ROOT, "\"%s\": %s", field, value));
            index(index, "" + (++docId), doc);
            refresh(index);
        }
        index("index", "1", String.format(Locale.ROOT, """
            {
              %s
            }
            """, String.join(",\n", mainProperties)));
        refresh("index");
    }

    private String sampleDataFor(DataType type) {
        return switch (type) {
            case KEYWORD -> "\"key\"";
            case TEXT -> "\"key text\"";
            case INTEGER -> "1";
            case FLOAT, DOUBLE -> "1.0";
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }
}
