/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code SET unmapped_fields} for nested {@code item.extra} vs unsupported {@code extra} ({@code ip_range}).
 * Each parameter is one cell of the table; classified on the nested / unsupported-mapped index.
 */
public class UnmappedFieldsNestedVsUnsupportedExtraIT extends AbstractEsqlIntegTestCase {

    private static final Presence ABSENT = Presence.ABSENT;
    private static final Presence NULL = Presence.NULL;
    private static final Presence LOADED = Presence.LOADED;

    /**
     * @param mode {@code nullify} / {@code load} / {@code load_all}
     * @param mapping {@code unmapped_both} / {@code mapped_both} / {@code mapped_only_nested}
     * @param keep {@code *} or {@code x}
     * @param nested result on the nested index
     * @param unsupported result on the unsupported index
     */
    record Cell(String mode, String mapping, String keep, Presence nested, Presence unsupported) {
        @Override
        public String toString() {
            return mode + " " + mapping + " KEEP " + keep + " → nested=" + nested + " unsupported=" + unsupported;
        }
    }

    enum Presence {
        ABSENT,
        NULL,
        LOADED
    }

    @ParametersFactory(argumentFormatting = "{0}")
    public static List<Object[]> params() {
        // ⚠️ means nested does not match unsupported; we still assert the current result.
        return List.of(
            // mode, mapping, KEEP, nested, unsupported
            cell("nullify", "unmapped_both", "*", ABSENT, ABSENT),
            cell("nullify", "unmapped_both", "x", NULL, NULL),
            cell("nullify", "mapped_both", "*", NULL, NULL),
            cell("nullify", "mapped_both", "x", NULL, NULL),
            // ⚠️ KEEP * drops the column; unsupported is null.
            cell("nullify", "mapped_only_nested", "*", ABSENT, NULL),
            cell("nullify", "mapped_only_nested", "x", NULL, NULL),

            cell("load", "unmapped_both", "*", ABSENT, ABSENT),
            cell("load", "unmapped_both", "x", LOADED, LOADED),
            cell("load", "mapped_both", "*", NULL, NULL),
            cell("load", "mapped_both", "x", NULL, NULL),
            // ⚠️ KEEP * drops the column; unsupported is null.
            cell("load", "mapped_only_nested", "*", ABSENT, NULL),
            cell("load", "mapped_only_nested", "x", NULL, NULL),

            cell("load_all", "unmapped_both", "*", LOADED, LOADED),
            cell("load_all", "unmapped_both", "x", LOADED, LOADED),
            cell("load_all", "mapped_both", "*", NULL, NULL),
            cell("load_all", "mapped_both", "x", NULL, NULL),
            // ⚠️ LOAD_ALL invents the column from _source; unsupported is null.
            cell("load_all", "mapped_only_nested", "*", LOADED, NULL),
            cell("load_all", "mapped_only_nested", "x", NULL, NULL)
        );
    }

    private static Object[] cell(String mode, String mapping, String keep, Presence nested, Presence unsupported) {
        return new Object[] { new Cell(mode, mapping, keep, nested, unsupported) };
    }

    private final Cell cell;

    public UnmappedFieldsNestedVsUnsupportedExtraIT(Cell cell) {
        this.cell = cell;
    }

    public void testCell() {
        assumeTrue("Requires SET unmapped_fields", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        if (cell.mode.equals("load_all")) {
            assumeTrue("Requires unmapped_fields=\"load_all\"", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        }
        boolean extraMappedOnHidden = switch (cell.mapping) {
            case "unmapped_both" -> false;
            case "mapped_both", "mapped_only_nested" -> true;
            default -> throw new IllegalArgumentException("unknown mapping " + cell.mapping);
        };
        boolean extraMappedOnOther = switch (cell.mapping) {
            case "mapped_both" -> true;
            case "unmapped_both", "mapped_only_nested" -> false;
            default -> throw new IllegalArgumentException("unknown mapping " + cell.mapping);
        };
        boolean keepStar = cell.keep.equals("*");
        String[] nested = createExtraLeafIndices("nest_", "obj_", extraMappedOnHidden, extraMappedOnOther);
        String[] unsupported = createUnsupportedExtraIndices("unsup_", "plain_", extraMappedOnHidden, extraMappedOnOther);
        String prefix = "SET unmapped_fields=\"" + cell.mode + "\"; ";
        String nestedKeep = keepStar ? "*" : "id, item.extra";
        String unsupportedKeep = keepStar ? "*" : "id, extra";
        assertThat(
            presence(prefix + "FROM " + nested[0] + ", " + nested[1] + " | KEEP " + nestedKeep + " | SORT id", "item.extra"),
            equalTo(cell.nested)
        );
        assertThat(
            presence(prefix + "FROM " + unsupported[0] + ", " + unsupported[1] + " | KEEP " + unsupportedKeep + " | SORT id", "extra"),
            equalTo(cell.unsupported)
        );
    }

    private String[] createExtraLeafIndices(String nestPrefix, String objPrefix, boolean extraMappedOnNested, boolean extraMappedOnObject) {
        String nested = nestPrefix + randomIdentifier();
        String object = objPrefix + randomIdentifier();
        createIndex(nested, extraLeafMapping(true, extraMappedOnNested));
        createIndex(object, extraLeafMapping(false, extraMappedOnObject));
        client().prepareBulk()
            .add(prepareIndexJson(nested, "0", """
                {
                  "id": "n00",
                  "item": [
                    {
                      "value": 100,
                      "extra": "from-nested-0"
                    }
                  ]
                }"""))
            .add(prepareIndexJson(nested, "1", """
                {
                  "id": "n01",
                  "item": [
                    {
                      "value": 101,
                      "extra": "from-nested-1"
                    }
                  ]
                }"""))
            .add(prepareIndexJson(object, "0", """
                {
                  "id": "o00",
                  "item": {
                    "value": 1,
                    "extra": "from-object-0"
                  }
                }"""))
            .add(prepareIndexJson(object, "1", """
                {
                  "id": "o01",
                  "item": {
                    "value": 2,
                    "extra": "from-object-1"
                  }
                }"""))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        return new String[] { nested, object };
    }

    private static String extraLeafMapping(boolean nestedItem, boolean extraMapped) {
        return Strings.format("""
            {
              "dynamic": false,
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "item": {
                  %s
                  "dynamic": false,
                  "properties": {
                    "value": {
                      "type": "%s"
                    }%s
                  }
                }
              }
            }""", nestedItem ? "\"type\": \"nested\"," : "", nestedItem ? "integer" : "long", extraMapped ? """
                    ,
                    "extra": {
                      "type": "keyword"
                    }""" : "");
    }

    private String[] createUnsupportedExtraIndices(String hiddenPrefix, String otherPrefix, boolean mappedOnHidden, boolean mappedOnOther) {
        String hidden = hiddenPrefix + randomIdentifier();
        String other = otherPrefix + randomIdentifier();
        createIndex(hidden, unsupportedExtraMapping(mappedOnHidden));
        createIndex(other, unsupportedExtraMapping(mappedOnOther));
        client().prepareBulk()
            .add(prepareIndexJson(hidden, "0", """
                {
                  "id": "n00",
                  "extra": "192.168.0.0/24"
                }"""))
            .add(prepareIndexJson(hidden, "1", """
                {
                  "id": "n01",
                  "extra": "192.168.1.0/24"
                }"""))
            .add(prepareIndexJson(other, "0", """
                {
                  "id": "o00",
                  "extra": "10.0.0.0/8"
                }"""))
            .add(prepareIndexJson(other, "1", """
                {
                  "id": "o01",
                  "extra": "10.1.0.0/16"
                }"""))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        return new String[] { hidden, other };
    }

    private static String unsupportedExtraMapping(boolean extraMapped) {
        return extraMapped
            ? """
                {
                  "dynamic": false,
                  "properties": {
                    "id": {
                      "type": "keyword"
                    },
                    "extra": {
                      "type": "ip_range"
                    }
                  }
                }"""
            : """
                {
                  "dynamic": false,
                  "properties": {
                    "id": {
                      "type": "keyword"
                    }
                  }
                }""";
    }

    private Presence presence(String query, String field) {
        try (var resp = run(query)) {
            assertFalse("query should not be partial: " + resp.getExecutionInfo(), resp.isPartial());
            List<String> names = resp.columns().stream().map(ColumnInfo::name).toList();
            int fieldIdx = names.indexOf(field);
            if (fieldIdx < 0) {
                return Presence.ABSENT;
            }
            int idIdx = names.indexOf("id");
            for (List<Object> row : getValuesList(resp)) {
                Object id = row.get(idIdx);
                if (id instanceof String s && s.startsWith("n") && row.get(fieldIdx) != null) {
                    return Presence.LOADED;
                }
            }
            return Presence.NULL;
        }
    }

    private void createIndex(String index, String mapping) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping(mapping)
        );
    }

    private IndexRequestBuilder prepareIndexJson(String index, String id, String json) {
        return client().prepareIndex(index).setId(id).setSource(json, XContentType.JSON);
    }
}
