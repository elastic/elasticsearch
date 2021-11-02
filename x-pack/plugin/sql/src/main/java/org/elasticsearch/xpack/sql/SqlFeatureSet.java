/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.sql.SqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.sql.plugin.SqlStatsAction;
import org.elasticsearch.xpack.sql.plugin.SqlStatsRequest;
import org.elasticsearch.xpack.sql.plugin.SqlStatsResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SqlFeatureSet implements XPackFeatureSet {

    private final Client client;

    @Inject
    public SqlFeatureSet(Client client) {
        this.client = client;
    }

    @Override
    public String name() {
        return XPackField.SQL;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        SqlStatsRequest request = new SqlStatsRequest();
        request.includeStats(true);
        client.execute(SqlStatsAction.INSTANCE, request, ActionListener.wrap(r -> {
            List<Counters> countersPerNode = r.getNodes()
                .stream()
                .map(SqlStatsResponse.NodeStatsResponse::getStats)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Counters mergedCounters = Counters.merge(countersPerNode);
            listener.onResponse(new SqlFeatureSetUsage(mergedCounters.toNestedMap()));
        }, listener::onFailure));
    }

}
