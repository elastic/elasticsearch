/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.xpack.esql.analysis.UnmappedResolution;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.EsqlStatement;
import org.elasticsearch.xpack.esql.plan.QuerySetting;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.SettingsValidationContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * The query settings a view definition sets for its own body with {@code SET} statements.
 * <p>
 * The caller's query settings stop at view boundaries ({@code SETTINGS_SCOPED_AT_VIEW_BOUNDARIES}): a view produces
 * the same rows whatever the caller's {@code SET} says. These are the settings the view <em>author</em> chose instead,
 * stored as part of the definition and applied to the body alone — exactly the three settings that scoping took away
 * from callers. They end at the next view boundary the same way the caller's do: a nested view is governed only by its
 * own definition's {@code SET}.
 * <p>
 * A setting a definition does not {@code SET} stays at its registry default inside the body, whatever the caller's
 * request or the cluster default says — a cluster-level default reaching into bodies would change view contents the
 * way a materialized view could not change.
 */
public record ViewQuerySettings(UnmappedResolution unmappedFields, boolean wildcardsMatchViews, boolean wildcardsMatchDatasets) {

    /** The settings of a view whose definition has no {@code SET} statements: everything at its registry default. */
    public static final ViewQuerySettings DEFAULTS = new ViewQuerySettings(UnmappedResolution.DEFAULT, false, false);

    /** The setting names a view definition may {@code SET} — the ones scoped at view boundaries for callers. */
    private static final Set<String> ALLOWED = Set.of(
        QuerySettings.UNMAPPED_FIELDS.name(),
        QuerySettings.WILDCARDS_MATCH_VIEWS.name(),
        QuerySettings.WILDCARDS_MATCH_DATASETS.name()
    );

    /**
     * Settings for a view branch. A view branch has a name; a definition that did not {@code SET} a mode contributes
     * {@link #DEFAULTS}, which is the registry default and not the cluster default.
     */
    public static ViewQuerySettings forView(String viewName, Map<String, ViewQuerySettings> viewSettings) {
        assert viewName != null : "a view branch has no view name to read settings from";
        ViewQuerySettings settings = viewSettings.get(viewName);
        return settings == null ? DEFAULTS : settings;
    }

    /**
     * Reads the view-scoped settings from a parsed view definition. Call {@link #validate} first; this assumes the
     * statement carries only allowed settings.
     */
    public static ViewQuerySettings from(EsqlStatement statement) {
        return new ViewQuerySettings(
            statement.setting(QuerySettings.UNMAPPED_FIELDS),
            statement.setting(QuerySettings.WILDCARDS_MATCH_VIEWS),
            statement.setting(QuerySettings.WILDCARDS_MATCH_DATASETS)
        );
    }

    /**
     * Rejects {@code SET} statements a view definition may not carry: any setting outside {@link #ALLOWED}, a value
     * that would not validate in a query, and {@code unmapped_fields="LOAD_ALL"}, whose machinery (the
     * {@code _unmapped_fields} column and its exchange plumbing) is query-global and not yet scopable to one view's body.
     */
    public static void validate(List<QuerySetting> settings, EsqlStatement statement) {
        if (settings == null || settings.isEmpty()) {
            return;
        }
        List<QuerySetting> rejected = new ArrayList<>();
        for (QuerySetting setting : settings) {
            if (ALLOWED.contains(setting.name()) == false) {
                rejected.add(setting);
            }
        }
        if (rejected.isEmpty() == false) {
            String names = rejected.stream().map(QuerySetting::name).distinct().sorted().collect(joining(", "));
            throw new ParsingException(
                rejected.get(0).source(),
                "SET [{}] is not allowed in views; only {} are",
                names,
                ALLOWED.stream().sorted().toList()
            );
        }
        // Same type and value checks as a query SET. Snapshot is forced on so LOAD_ALL passes that gate and fails
        // below with the view-specific message on every build, snapshot or not.
        QuerySettings.validate(statement, new SettingsValidationContext(false, true));
        if (statement.setting(QuerySettings.UNMAPPED_FIELDS) == UnmappedResolution.LOAD_ALL) {
            QuerySetting setting = settings.stream()
                .filter(s -> s.name().equals(QuerySettings.UNMAPPED_FIELDS.name()))
                .findFirst()
                .orElseThrow();
            throw new ParsingException(setting.source(), "SET unmapped_fields=\"LOAD_ALL\" is not supported in views");
        }
    }

    /**
     * Whether this view's branch must survive analysis so the analyzer can apply {@link #unmappedFields} to the body
     * — the view-side counterpart of {@code ViewBoundaryPreservation#analysisRequiresViewBranches}.
     */
    public boolean requiresViewBranches() {
        return unmappedFields != UnmappedResolution.DEFAULT;
    }

    /**
     * Whether this view must keep its identity through resolution and compaction even as a pass-through — the
     * view-side counterpart of {@code ViewBoundaryPreservation#settingsRequireViewIdentity}, for the view's own
     * settings instead of the caller's.
     */
    public boolean requiresViewIdentity() {
        return ViewBoundaryPreservation.settingsRequireViewIdentity(unmappedFields, wildcardsMatchDatasets);
    }

    /**
     * At most this many names are listed in one {@link #bracketedList}; the rest are counted. A wide wildcard can match hundreds of
     * names, and listing them all makes the {@code Warning} header too large for some clients.
     */
    public static final int MAX_LISTED_NAMES_IN_VIEW_WARNINGS = 20;

    /**
     * Where a {@code wildcards_match_*=true} stopped: at the boundary of {@code viewName}, whose definition does not {@code SET}
     * it. {@code settingView} is the view whose own definition {@code SET} it, or {@code null} when the caller's query did; a
     * user reading the warning needs to know whether the {@code SET} is theirs to change.
     */
    public record WildcardsStop(@Nullable String settingView, String viewName) {}

    /**
     * The warning for a {@code SET <setting>=true} that stopped as {@code stop} says, at a view whose body has wildcards that
     * would otherwise have matched {@code names}, which are views or datasets as {@code kind} says. A user who knows the view's
     * contents may expect the setting to widen them. {@code circularNames} are the views the wildcards also match that enclose
     * them, the view itself included: the view definition would have to exclude those, since matching one is a circular reference.
     */
    public static String wildcardsStoppedAtViewWarning(
        String setting,
        WildcardsStop stop,
        String kind,
        List<String> names,
        List<String> circularNames
    ) {
        assert names.isEmpty() == false : "nothing to warn about";
        String circular = circularNames.isEmpty()
            ? ""
            : Strings.format(
                ", excluding view%s %s from its wildcards since matching %s would be a circular reference",
                circularNames.size() == 1 ? "" : "s",
                bracketedList(circularNames),
                circularNames.size() == 1 ? "it" : "them"
            );
        return Strings.format(
            "SET %s=true%s does not affect the contents of view [%s], so its wildcards did not match %s%s %s; "
                + "use that setting inside the view definition if you wish to change the view behaviour%s",
            setting,
            stop.settingView() == null ? "" : " in view [" + stop.settingView() + "]",
            stop.viewName(),
            kind,
            names.size() == 1 ? "" : "s",
            bracketedList(names),
            circular
        );
    }

    /**
     * {@code names} as {@code [a], [b]} for the warnings about settings stopped at a view boundary, capped at
     * {@link #MAX_LISTED_NAMES_IN_VIEW_WARNINGS} names followed by {@code and [N] more}.
     */
    public static String bracketedList(List<String> names) {
        int listed = Math.min(names.size(), MAX_LISTED_NAMES_IN_VIEW_WARNINGS);
        StringBuilder list = new StringBuilder();
        for (int i = 0; i < listed; i++) {
            list.append(i == 0 ? "[" : ", [").append(names.get(i)).append("]");
        }
        if (names.size() > listed) {
            list.append(" and [").append(names.size() - listed).append("] more");
        }
        return list.toString();
    }
}
