/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * What the suites that run the csv-spec queries with {@code unmapped_fields="load"} again with {@code "LOAD_ALL"} share.
 * <p>
 * {@code LOAD_ALL} does what {@code LOAD} does for the fields a query names, and returns the others, which {@code LOAD} leaves out, in
 * the extra {@code _unmapped_fields} column. So a query is not changed apart from the setting, and the column is ignored when the
 * results are compared.
 * <p>
 * A test that is skipped is listed below with the reason. Remove it from its list when {@code LOAD_ALL} supports what it needs, so that
 * it runs: the lists show what is left to support.
 */
public final class LoadAllFromLoad {

    /** The spec files that have queries with {@code unmapped_fields="load"}, for the suites that generate one class per file. */
    public static final String[] SPEC_FILES = {
        "enrich.csv-spec",
        "flattened-unmapped-load.csv-spec",
        "flattened-unmapped-sink-load.csv-spec",
        "in_subquery.csv-spec",
        "nested_subquery.csv-spec",
        "subquery.csv-spec",
        "unmapped-load.csv-spec",
        "unmapped-punk-singles.csv-spec",
        "unmapped-type-conflicts.csv-spec",
        "views.csv-spec" };

    /** Tests with {@code TS}, which {@code LOAD_ALL} does not support. */
    private static final Set<String> NEEDS_TS = Set.of(
        "unmapped-load.evalOnMaxOverTime",
        "unmapped-load.explicitLastOverTimeUnmappedTsIndex",
        "unmapped-load.loadViewTsBranchingStatsLoadMode",
        "unmapped-load.loadViewTsMaxBytesLoadMode",
        "unmapped-load.maxOverTimeWithFiltering",
        "unmapped-load.unmappedNumericFromK8sSyntheticTsIndex",
        "unmapped-type-conflicts.tsPartiallyMappedNonKeywordFieldWithCast"
    );

    /**
     * Tests where {@code LOAD_ALL} is meant to differ: a field that one branch of a subquery names is loaded in that branch only by
     * {@code LOAD}, which null-fills it in the others, and in all of them by {@code LOAD_ALL}.
     */
    private static final Set<String> DIFFERS_BY_DESIGN = Set.of(
        "unmapped-load.fillNullAllColumnsInSubqueryBranchWithDifferentColumnsUnderLoad",
        "unmapped-load.loadSubquerySortRefInOneBranchTrailingWildcardKeepSurfacesUnmapped",
        "unmapped-load.loadUnmappedFieldInOneSubqueryBranchNoOuterKeep"
    );

    public static final String UNMAPPED_FIELDS_COLUMN = "_unmapped_fields";

    private static final Pattern LOAD_SETTING = Pattern.compile("(?i)(unmapped_fields\\s*=\\s*)\"load\"");
    /** The setting as a warning spells it, which may escape the quotes: it names the mode, which the rewrite changes. */
    private static final Pattern MODE_IN_WARNING = Pattern.compile("(?i)unmapped_fields\\s*=\\s*\\\\?\"(?:load_all|load)\\\\?\"");
    /** The mode the setting of a warning has when it is the first thing the warning says, as for a view whose rows are not loaded. */
    private static final Pattern LEADING_MODE_IN_WARNING = Pattern.compile("(?i)^(SET unmapped_fields=\\\\?\")load(\\\\?\")");

    private LoadAllFromLoad() {}

    /** Does the query set {@code unmapped_fields="load"}? */
    public static boolean usesLoad(String query) {
        return LOAD_SETTING.matcher(query).find();
    }

    /** The query with {@code unmapped_fields="load"} replaced by {@code "LOAD_ALL"}. */
    public static String toLoadAll(String query) {
        return LOAD_SETTING.matcher(query).replaceAll("$1\"LOAD_ALL\"");
    }

    /** Why the test is not run with {@code LOAD_ALL}, or {@code null} if it is. */
    public static String skipReason(String testId) {
        if (NEEDS_TS.contains(testId)) {
            return "LOAD_ALL does not support TS";
        }
        if (DIFFERS_BY_DESIGN.contains(testId)) {
            return "LOAD_ALL differs from LOAD by design here: it loads the field in the other branches too";
        }
        return null;
    }

    /** A warning with the name of the mode left out, to compare the warnings of {@code LOAD} and {@code LOAD_ALL}. */
    public static String withoutMode(String warning) {
        return MODE_IN_WARNING.matcher(warning).replaceAll("unmapped_fields=<mode>");
    }

    /**
     * An expected warning of the {@code LOAD} query as the {@code LOAD_ALL} query warns. Only the setting that the query has, which is
     * the first thing a warning says, changes, to the lower-case name of the mode as a warning spells it: the advice that a view must {@code SET unmapped_fields="load"} stays as it is.
     */
    public static String expectedWarning(String warning) {
        return LEADING_MODE_IN_WARNING.matcher(warning).replaceFirst("$1load_all$2");
    }

    /**
     * The columns and rows of a response without the {@code _unmapped_fields} column, unless the spec expects the column.
     *
     * @param columns the columns of the response, each a map with its {@code name} and {@code type}
     */
    public static Results withoutUnmappedFieldsColumn(boolean expectsColumn, List<Map<String, String>> columns, List<List<Object>> values) {
        int column = -1;
        for (int i = 0; i < columns.size(); i++) {
            if (UNMAPPED_FIELDS_COLUMN.equals(columns.get(i).get("name"))) {
                column = i;
                break;
            }
        }
        if (column < 0 || expectsColumn) {
            return new Results(columns, values);
        }
        List<Map<String, String>> keptColumns = new ArrayList<>(columns);
        keptColumns.remove(column);
        List<List<Object>> keptValues = new ArrayList<>(values.size());
        for (List<Object> row : values) {
            List<Object> copy = new ArrayList<>(row);
            copy.remove(column);
            keptValues.add(copy);
        }
        return new Results(keptColumns, keptValues);
    }

    public record Results(List<Map<String, String>> columns, List<List<Object>> values) {}
}
