/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.eql.EqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.eql.plugin.EqlStatsAction;
import org.elasticsearch.xpack.eql.plugin.EqlStatsRequest;
import org.elasticsearch.xpack.eql.plugin.EqlStatsResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class EqlFeatureSet implements XPackFeatureSet {

    private final Client client;

    @Inject
    public EqlFeatureSet(Client client) {
        this.client = client;
    }

    @Override
    public String name() {
        return XPackField.EQL;
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
        EqlStatsRequest request = new EqlStatsRequest();
        request.includeStats(true);
        client.execute(EqlStatsAction.INSTANCE, request, ActionListener.wrap(r -> {
            List<Counters> countersPerNode = r.getNodes()
                .stream()
                .map(EqlStatsResponse.NodeStatsResponse::getStats)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            Counters mergedCounters = Counters.merge(countersPerNode);
            listener.onResponse(new EqlFeatureSetUsage(mergedCounters.toNestedMap()));
        }, listener::onFailure));
    }

}
