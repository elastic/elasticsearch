/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code unmapped_fields="LOAD_ALL"} expands at most {@code esql.load_all.max_fields} fields discovered in {@code _source} into
 * columns, and the limit can be changed on a running cluster.
 */
public class LoadAllMaxFieldsIT extends AbstractEsqlIntegTestCase {
    private static final String INDEX = "load-all-max-fields";
    private static final String QUERY = "SET unmapped_fields=\"LOAD_ALL\"; FROM " + INDEX + " | SORT mapped | LIMIT 10";
    private static final int UNMAPPED_FIELDS = 10;

    public void testLimitFollowsTheClusterSetting() {
        assumeTrue(
            "requires the LOAD_ALL field limit setting",
            EsqlCapabilities.Cap.OPTIONAL_FIELDS_LOAD_ALL_MAX_FIELDS_SETTING.isEnabled()
        );
        // dynamic:false keeps the extra source fields out of the mapping, so LOAD_ALL is the only thing that surfaces them.
        assertAcked(client().admin().indices().prepareCreate(INDEX).setSettings(indexSettings(1, 0)).setMapping("""
            { "dynamic": false, "properties": { "mapped": { "type": "long" } } }"""));
        Object[] source = new Object[2 + 2 * UNMAPPED_FIELDS];
        source[0] = "mapped";
        source[1] = 1;
        for (int i = 0; i < UNMAPPED_FIELDS; i++) {
            source[2 + 2 * i] = unmappedField(i);
            source[3 + 2 * i] = "value" + i;
        }
        indexDoc(INDEX, "1", source);
        refresh(INDEX);

        String key = PlannerSettings.LOAD_ALL_MAX_FIELDS.getKey();
        try {
            assertThat("every field is expanded below the default limit", columnNames(), equalTo(expectedColumns(UNMAPPED_FIELDS)));

            int lowered = between(1, UNMAPPED_FIELDS - 1);
            updateClusterSettings(Settings.builder().put(key, lowered));
            assertThat("only the alphabetically first fields are expanded", columnNames(), equalTo(expectedColumns(lowered)));

            updateClusterSettings(Settings.builder().put(key, UNMAPPED_FIELDS));
            assertThat("a limit that fits all of them expands all of them", columnNames(), equalTo(expectedColumns(UNMAPPED_FIELDS)));
        } finally {
            updateClusterSettings(Settings.builder().putNull(key));
        }
        assertThat("removing the setting restores the default", columnNames(), equalTo(expectedColumns(UNMAPPED_FIELDS)));
    }

    private List<String> columnNames() {
        try (var response = run(QUERY)) {
            return response.columns().stream().map(column -> column.name()).toList();
        }
    }

    /** The mapped column, then the first {@code count} unmapped fields in alphabetical order. */
    private static List<String> expectedColumns(int count) {
        List<String> columns = new ArrayList<>();
        columns.add("mapped");
        for (int i = 0; i < count; i++) {
            columns.add(unmappedField(i));
        }
        return columns;
    }

    /** Zero-padded so that the alphabetical order is the numeric one. */
    private static String unmappedField(int i) {
        return String.format(Locale.ROOT, "unmapped_%02d", i);
    }
}
