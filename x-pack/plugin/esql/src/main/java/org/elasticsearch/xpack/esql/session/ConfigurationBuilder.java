/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.time.Instant;
import java.util.Locale;
import java.util.Map;

/**
 * Builder for {@link Configuration}. Use {@link #ConfigurationBuilder(Configuration)} to copy an
 * existing configuration and override individual fields before calling {@link #build()}.
 */
public class ConfigurationBuilder {

    private Instant now;
    private String clusterName;
    private String username;
    private ResolvedSettings resolvedSettings;
    private QueryPragmas pragmas;
    private int resultTruncationMaxSizeRegular;
    private int resultTruncationDefaultSizeRegular;
    private int resultTruncationMaxSizeTimeseries;
    private int resultTruncationDefaultSizeTimeseries;
    private Locale locale;
    private String query;
    private boolean profile;
    private boolean allowPartialResults;
    private boolean explainOnly;
    private Map<String, Map<String, Column>> tables;
    private long queryStartTimeNanos;
    private Map<String, String> viewQueries;

    public ConfigurationBuilder(Configuration configuration) {
        now = configuration.now();
        clusterName = configuration.clusterName();
        username = configuration.username();
        resolvedSettings = configuration.resolvedSettings();
        pragmas = configuration.pragmas();
        resultTruncationMaxSizeRegular = configuration.resultTruncationMaxSize(false);
        resultTruncationDefaultSizeRegular = configuration.resultTruncationDefaultSize(false);
        resultTruncationMaxSizeTimeseries = configuration.resultTruncationMaxSize(true);
        resultTruncationDefaultSizeTimeseries = configuration.resultTruncationDefaultSize(true);
        locale = configuration.locale();
        query = configuration.query();
        profile = configuration.profile();
        allowPartialResults = configuration.allowPartialResults();
        explainOnly = configuration.explainOnly();
        tables = configuration.tables();
        queryStartTimeNanos = configuration.queryStartTimeNanos();
        viewQueries = configuration.viewQueries();
    }

    public ConfigurationBuilder now(Instant now) {
        this.now = now;
        return this;
    }

    public ConfigurationBuilder clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public ConfigurationBuilder username(String username) {
        this.username = username;
        return this;
    }

    public ConfigurationBuilder pragmas(QueryPragmas pragmas) {
        this.pragmas = pragmas;
        return this;
    }

    public ConfigurationBuilder resultTruncationMaxSizeRegular(int resultTruncationMaxSizeRegular) {
        this.resultTruncationMaxSizeRegular = resultTruncationMaxSizeRegular;
        return this;
    }

    public ConfigurationBuilder resultTruncationDefaultSizeRegular(int resultTruncationDefaultSizeRegular) {
        this.resultTruncationDefaultSizeRegular = resultTruncationDefaultSizeRegular;
        return this;
    }

    public ConfigurationBuilder resultTruncationMaxSizeTimeseries(int resultTruncationMaxSizeTimeseries) {
        this.resultTruncationMaxSizeTimeseries = resultTruncationMaxSizeTimeseries;
        return this;
    }

    public ConfigurationBuilder resultTruncationDefaultSizeTimeseries(int resultTruncationDefaultSizeTimeseries) {
        this.resultTruncationDefaultSizeTimeseries = resultTruncationDefaultSizeTimeseries;
        return this;
    }

    public ConfigurationBuilder locale(Locale locale) {
        this.locale = locale;
        return this;
    }

    public ConfigurationBuilder query(String query) {
        this.query = query;
        return this;
    }

    public ConfigurationBuilder profile(boolean profile) {
        this.profile = profile;
        return this;
    }

    public ConfigurationBuilder allowPartialResults(boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
        return this;
    }

    public ConfigurationBuilder explainOnly(boolean explainOnly) {
        this.explainOnly = explainOnly;
        return this;
    }

    public ConfigurationBuilder tables(Map<String, Map<String, Column>> tables) {
        this.tables = tables;
        return this;
    }

    public ConfigurationBuilder queryStartTimeNanos(long queryStartTimeNanos) {
        this.queryStartTimeNanos = queryStartTimeNanos;
        return this;
    }

    public ConfigurationBuilder resolvedSettings(ResolvedSettings resolvedSettings) {
        this.resolvedSettings = resolvedSettings;
        return this;
    }

    /** Override one {@link QuerySettingDef} value on the resolved-settings view. Generic — caller names the setting. */
    public <T> ConfigurationBuilder setting(QuerySettingDef<T> def, T value) {
        this.resolvedSettings = resolvedSettings.withOverride(def, value);
        return this;
    }

    public ConfigurationBuilder viewQueries(Map<String, String> viewQueries) {
        this.viewQueries = viewQueries;
        return this;
    }

    public Configuration build() {
        return new Configuration(
            now,
            locale,
            username,
            clusterName,
            pragmas,
            resultTruncationMaxSizeRegular,
            resultTruncationDefaultSizeRegular,
            query,
            profile,
            tables,
            queryStartTimeNanos,
            allowPartialResults,
            resultTruncationMaxSizeTimeseries,
            resultTruncationDefaultSizeTimeseries,
            resolvedSettings,
            viewQueries,
            explainOnly
        );
    }
}
