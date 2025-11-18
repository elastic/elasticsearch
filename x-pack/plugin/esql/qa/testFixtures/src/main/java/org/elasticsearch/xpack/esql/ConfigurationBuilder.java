/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;

/**
 * Builder to modify configurations on tests.
 * <p>
 *     The {@link Configuration#now()} field is actually modified, so built configurations will also differ there.
 * </p>
 */
public class ConfigurationBuilder {

    private String clusterName;
    private String username;
    private ZoneId zoneId;

    private QueryPragmas pragmas;

    private int resultTruncationMaxSizeRegular;
    private int resultTruncationDefaultSizeRegular;
    private int resultTruncationMaxSizeTimeseries;
    private int resultTruncationDefaultSizeTimeseries;

    private Locale locale;

    private String query;

    private boolean profile;
    private boolean allowPartialResults;

    private Map<String, Map<String, Column>> tables;
    private long queryStartTimeNanos;

    public ConfigurationBuilder(Configuration configuration) {
        clusterName = configuration.clusterName();
        username = configuration.username();
        zoneId = configuration.zoneId();
        pragmas = configuration.pragmas();
        resultTruncationMaxSizeRegular = configuration.resultTruncationMaxSize(false);
        resultTruncationDefaultSizeRegular = configuration.resultTruncationDefaultSize(false);
        resultTruncationMaxSizeTimeseries = configuration.resultTruncationMaxSize(true);
        resultTruncationDefaultSizeTimeseries = configuration.resultTruncationDefaultSize(true);
        locale = configuration.locale();
        query = configuration.query();
        profile = configuration.profile();
        allowPartialResults = configuration.allowPartialResults();
        tables = configuration.tables();
        queryStartTimeNanos = configuration.queryStartTimeNanos();
    }

    public ConfigurationBuilder clusterName(String clusterName) {
        this.clusterName = clusterName;
        return this;
    }

    public ConfigurationBuilder username(String username) {
        this.username = username;
        return this;
    }

    public ConfigurationBuilder zoneId(ZoneId zoneId) {
        this.zoneId = zoneId;
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

    public ConfigurationBuilder tables(Map<String, Map<String, Column>> tables) {
        this.tables = tables;
        return this;
    }

    public ConfigurationBuilder queryStartTimeNanos(long queryStartTimeNanos) {
        this.queryStartTimeNanos = queryStartTimeNanos;
        return this;
    }

    public Configuration build() {
        return new Configuration(
            zoneId,
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
            resultTruncationDefaultSizeTimeseries
        );
    }
}
