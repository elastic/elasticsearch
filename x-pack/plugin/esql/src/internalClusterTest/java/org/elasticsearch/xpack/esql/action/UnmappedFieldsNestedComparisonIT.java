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
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code SET unmapped_fields} for nested {@code item.extra} vs a nested parent with no declared subfields vs unsupported
 * {@code extra} ({@code ip_range}) vs the same {@code item.extra} with no mapping at all vs {@code item.extra} under a
 * {@code flattened} parent. Each parameter is one cell of the table; classified on each scenario's special index - the
 * nested / bare-nested / unsupported / never-mapped / flattened one, paired with a plain object index.
 * <p>
 * Nested subfields are hidden from field caps ({@code -nested}). A leaf <b>declared</b> under a nested parent is always
 * null: it is not loaded even in load modes, so that value does not change shape when ES|QL gains real nested support
 * (loading it as an unmapped keyword now would make that a breaking change). A leaf that is <b>not declared</b> in the
 * mapping behaves like any other unmapped field: load modes read it from {@code _source} and {@code nullify} nulls it.
 * Unsupported fields are mapped, so wherever the mapping has them they stay null. Flattened sub-keys are also invisible
 * to field caps, but they are not treated as unmapped: the {@code Verifier} rejects loading a subfield of a flattened
 * parent, and {@code nullify} nulls it.
 */
public class UnmappedFieldsNestedComparisonIT extends AbstractEsqlIntegTestCase {

    private static final Presence ABSENT = Presence.ABSENT;
    private static final Presence NULL = Presence.NULL;
    private static final Presence LOADED = Presence.LOADED;
    private static final Presence VERIFIER_ERROR = Presence.VERIFIER_ERROR;

    /**
     * @param mode {@code nullify} / {@code load} / {@code load_all}
     * @param mapping {@code unmapped_both} / {@code mapped_both} / {@code mapped_only_special}. Only the nested and
     *     unsupported scenarios can declare the leaf on their special index; for the rest, {@code mapped_only_special}
     *     behaves like {@code unmapped_both}.
     * @param keep {@code *} or {@code x}
     * @param nested result on the nested index
     * @param nestedNoField result on the index where {@code item} is nested but declares no subfields at all; must equal
     *     {@code noField}
     * @param unsupported result on the unsupported index
     * @param noField result on the index where the leaf is not mapped at all
     * @param flattened result on the index where {@code item} is mapped {@code flattened}
     */
    record Cell(
        String mode,
        String mapping,
        String keep,
        Presence nested,
        Presence nestedNoField,
        Presence unsupported,
        Presence noField,
        Presence flattened
    ) {
        @Override
        public String toString() {
            return mode
                + " "
                + mapping
                + " KEEP "
                + keep
                + " → nested="
                + nested
                + " nestedNoField="
                + nestedNoField
                + " unsupported="
                + unsupported
                + " noField="
                + noField
                + " flattened="
                + flattened;
        }
    }

    enum Presence {
        ABSENT,
        NULL,
        LOADED,
        VERIFIER_ERROR
    }

    /**
     * Dimensions: {@code mode} is the {@code SET unmapped_fields} value; {@code mapping} is where the leaf is declared
     * (nowhere, both indices, or only the special index); {@code keep} is whether the query names the leaf ({@code x})
     * or uses {@code KEEP *}. The remaining columns are the expected result per special-index scenario:
     * <ul>
     * <li>nested: a leaf declared under the nested parent is always null - never loaded, so real nested support can
     *     change what it returns without breaking anyone. An undeclared leaf ({@code unmapped_both}) follows the
     *     unmapped-field rules like any other.</li>
     * <li>nestedNoField: nested parent that declares no subfields at all. Must always equal noField: nothing is
     *     declared, so it is plain unmapped loading.</li>
     * <li>noField: the same leaf simply not mapped on the special index.</li>
     * <li>flattened: the Verifier rejects loading the sub-key, so cells that would load it error instead.
     *     Exception: {@code LOAD_ALL} + {@code KEEP *} with nothing referencing it - {@code _source} discovery surfaces it.</li>
     * </ul>
     */
    @ParametersFactory(argumentFormatting = "%1$s")
    public static List<Object[]> params() {
        return List.of(
            // mode, mapping, KEEP, nested, nestedNoField, unsupported, noField, flattened
            cell("nullify", "unmapped_both", "*", ABSENT, ABSENT, ABSENT, ABSENT, ABSENT),
            cell("nullify", "unmapped_both", "x", NULL, NULL, NULL, NULL, NULL),
            cell("nullify", "mapped_both", "*", NULL, NULL, NULL, NULL, NULL),
            cell("nullify", "mapped_both", "x", NULL, NULL, NULL, NULL, NULL),
            cell("nullify", "mapped_only_special", "*", ABSENT, ABSENT, NULL, ABSENT, ABSENT),
            cell("nullify", "mapped_only_special", "x", NULL, NULL, NULL, NULL, NULL),

            cell("load", "unmapped_both", "*", ABSENT, ABSENT, ABSENT, ABSENT, ABSENT),
            cell("load", "unmapped_both", "x", LOADED, LOADED, LOADED, LOADED, VERIFIER_ERROR),
            cell("load", "mapped_both", "*", NULL, LOADED, NULL, LOADED, VERIFIER_ERROR),
            cell("load", "mapped_both", "x", NULL, LOADED, NULL, LOADED, VERIFIER_ERROR),
            cell("load", "mapped_only_special", "*", ABSENT, ABSENT, NULL, ABSENT, ABSENT),
            cell("load", "mapped_only_special", "x", NULL, LOADED, NULL, LOADED, VERIFIER_ERROR),

            cell("load_all", "unmapped_both", "*", LOADED, LOADED, LOADED, LOADED, LOADED),
            cell("load_all", "unmapped_both", "x", LOADED, LOADED, LOADED, LOADED, VERIFIER_ERROR),
            cell("load_all", "mapped_both", "*", NULL, LOADED, NULL, LOADED, VERIFIER_ERROR),
            cell("load_all", "mapped_both", "x", NULL, LOADED, NULL, LOADED, VERIFIER_ERROR),
            cell("load_all", "mapped_only_special", "*", NULL, LOADED, NULL, LOADED, LOADED),
            cell("load_all", "mapped_only_special", "x", NULL, LOADED, NULL, LOADED, VERIFIER_ERROR)
        );
    }

    private static Object[] cell(
        String mode,
        String mapping,
        String keep,
        Presence nested,
        Presence nestedNoField,
        Presence unsupported,
        Presence noField,
        Presence flattened
    ) {
        return new Object[] { new Cell(mode, mapping, keep, nested, nestedNoField, unsupported, noField, flattened) };
    }

    private final Cell cell;

    public UnmappedFieldsNestedComparisonIT(Cell cell) {
        this.cell = cell;
    }

    public void testCell() {
        assumeTrue("Requires SET unmapped_fields", EsqlCapabilities.Cap.OPTIONAL_FIELDS_V5.isEnabled());
        if (cell.mode.equals("load_all")) {
            assumeTrue("Requires unmapped_fields=\"load_all\"", EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_V2.isEnabled());
        }
        boolean extraMappedOnSpecial = switch (cell.mapping) {
            case "unmapped_both" -> false;
            case "mapped_both", "mapped_only_special" -> true;
            default -> throw new IllegalArgumentException("unknown mapping " + cell.mapping);
        };
        boolean extraMappedOnOther = switch (cell.mapping) {
            case "mapped_both" -> true;
            case "unmapped_both", "mapped_only_special" -> false;
            default -> throw new IllegalArgumentException("unknown mapping " + cell.mapping);
        };
        boolean keepStar = cell.keep.equals("*");
        String[] nested = createExtraLeafIndices("nest_", "obj_", extraMappedOnSpecial, extraMappedOnOther);
        String[] nestedNoField = createNestedNoFieldExtraIndices("nnofld_", "nnoobj_", extraMappedOnOther);
        String[] unsupported = createUnsupportedExtraIndices("unsup_", "plain_", extraMappedOnSpecial, extraMappedOnOther);
        String[] noField = createNoFieldExtraIndices("nofld_", "noobj_", extraMappedOnOther);
        String[] flattened = createFlattenedExtraIndices("flat_", "flobj_", extraMappedOnOther);
        String prefix = "SET unmapped_fields=\"" + cell.mode + "\"; ";
        String nestedKeep = keepStar ? "*" : "id, item.extra";
        String unsupportedKeep = keepStar ? "*" : "id, extra";
        assertThat(
            presence(prefix + "FROM " + nested[0] + ", " + nested[1] + " | KEEP " + nestedKeep + " | SORT id", "item.extra"),
            equalTo(cell.nested)
        );
        assertThat(
            presence(prefix + "FROM " + nestedNoField[0] + ", " + nestedNoField[1] + " | KEEP " + nestedKeep + " | SORT id", "item.extra"),
            equalTo(cell.nestedNoField)
        );
        assertThat(
            presence(prefix + "FROM " + unsupported[0] + ", " + unsupported[1] + " | KEEP " + unsupportedKeep + " | SORT id", "extra"),
            equalTo(cell.unsupported)
        );
        assertThat(
            presence(prefix + "FROM " + noField[0] + ", " + noField[1] + " | KEEP " + nestedKeep + " | SORT id", "item.extra"),
            equalTo(cell.noField)
        );
        assertThat(
            presence(prefix + "FROM " + flattened[0] + ", " + flattened[1] + " | KEEP " + nestedKeep + " | SORT id", "item.extra"),
            equalTo(cell.flattened)
        );
    }

    private String[] createExtraLeafIndices(String nestPrefix, String objPrefix, boolean extraMappedOnNested, boolean extraMappedOnObject) {
        String nested = nestPrefix + randomIdentifier();
        String object = objPrefix + randomIdentifier();
        createIndex(nested, extraLeafMapping(true, extraMappedOnNested));
        createIndex(object, extraLeafMapping(false, extraMappedOnObject));
        indexItemDocs(nested, object);
        return new String[] { nested, object };
    }

    /**
     * Same shape as {@link #createExtraLeafIndices}, but the special index maps {@code item} as a plain object and never
     * maps {@code extra} - the plain unmapped-field baseline. Both indices map {@code item.value} as {@code long} so the
     * only difference from the nested scenario is the missing mapping.
     */
    private String[] createNoFieldExtraIndices(String specialPrefix, String otherPrefix, boolean extraMappedOnOther) {
        String special = specialPrefix + randomIdentifier();
        String other = otherPrefix + randomIdentifier();
        createIndex(special, extraLeafMapping(false, false));
        createIndex(other, extraLeafMapping(false, extraMappedOnOther));
        indexItemDocs(special, other);
        return new String[] { special, other };
    }

    /**
     * Same shape as {@link #createExtraLeafIndices}, but the special index maps {@code item} as a nested parent with no
     * declared subfields at all: neither {@code value} nor {@code extra} exist anywhere in its mapping.
     */
    private String[] createNestedNoFieldExtraIndices(String specialPrefix, String otherPrefix, boolean extraMappedOnOther) {
        String special = specialPrefix + randomIdentifier();
        String other = otherPrefix + randomIdentifier();
        createIndex(special, """
            {
              "dynamic": false,
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "item": {
                  "type": "nested",
                  "dynamic": false
                }
              }
            }""");
        createIndex(other, extraLeafMapping(false, extraMappedOnOther));
        indexItemDocs(special, other);
        return new String[] { special, other };
    }

    /**
     * Same shape as {@link #createExtraLeafIndices}, but the special index maps {@code item} as {@code flattened}, so
     * {@code item.extra} is a dynamic sub-key: invisible to field caps like a nested subfield, but resolvable on the
     * shard through the keyed flattened loader.
     */
    private String[] createFlattenedExtraIndices(String specialPrefix, String otherPrefix, boolean extraMappedOnOther) {
        String special = specialPrefix + randomIdentifier();
        String other = otherPrefix + randomIdentifier();
        createIndex(special, """
            {
              "dynamic": false,
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "item": {
                  "type": "flattened"
                }
              }
            }""");
        createIndex(other, extraLeafMapping(false, extraMappedOnOther));
        indexItemDocs(special, other);
        return new String[] { special, other };
    }

    private void indexItemDocs(String special, String other) {
        client().prepareBulk().add(prepareIndexJson(special, "0", """
            {
              "id": "n00",
              "item": [
                {
                  "value": 100,
                  "extra": "from-nested-0"
                }
              ]
            }""")).add(prepareIndexJson(special, "1", """
            {
              "id": "n01",
              "item": [
                {
                  "value": 101,
                  "extra": "from-nested-1"
                }
              ]
            }""")).add(prepareIndexJson(other, "0", """
            {
              "id": "o00",
              "item": {
                "value": 1,
                "extra": "from-object-0"
              }
            }""")).add(prepareIndexJson(other, "1", """
            {
              "id": "o01",
              "item": {
                "value": 2,
                "extra": "from-object-1"
              }
            }""")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
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

    private String[] createUnsupportedExtraIndices(
        String specialPrefix,
        String otherPrefix,
        boolean mappedOnSpecial,
        boolean mappedOnOther
    ) {
        String special = specialPrefix + randomIdentifier();
        String other = otherPrefix + randomIdentifier();
        createIndex(special, unsupportedExtraMapping(mappedOnSpecial));
        createIndex(other, unsupportedExtraMapping(mappedOnOther));
        client().prepareBulk().add(prepareIndexJson(special, "0", """
            {
              "id": "n00",
              "extra": "192.168.0.0/24"
            }""")).add(prepareIndexJson(special, "1", """
            {
              "id": "n01",
              "extra": "192.168.1.0/24"
            }""")).add(prepareIndexJson(other, "0", """
            {
              "id": "o00",
              "extra": "10.0.0.0/8"
            }""")).add(prepareIndexJson(other, "1", """
            {
              "id": "o01",
              "extra": "10.1.0.0/16"
            }""")).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        return new String[] { special, other };
    }

    private static String unsupportedExtraMapping(boolean extraMapped) {
        return extraMapped ? """
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
            }""" : """
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
            return presence(resp, field);
        } catch (VerificationException e) {
            // The only verifier rejection these scenarios can hit: loading a sub-key of a flattened parent.
            assertThat(e.getMessage(), containsString("is of flattened field type is not supported"));
            return Presence.VERIFIER_ERROR;
        }
    }

    private static Presence presence(EsqlQueryResponse resp, String field) {
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
