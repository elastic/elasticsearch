/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.eql.EqlFeatureSetUsage;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.eql.plugin.EqlPlugin;
import org.elasticsearch.xpack.eql.plugin.EqlStatsAction;
import org.elasticsearch.xpack.eql.plugin.EqlStatsRequest;
import org.elasticsearch.xpack.eql.plugin.EqlStatsResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class EqlFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Client client;
    
    @Inject
    public EqlFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState, Client client) {
        this.enabled = EqlPlugin.isEnabled(settings);
        this.licenseState = licenseState;
        this.client = client;
    }
    
    @Override
    public String name() {
        return XPackField.EQL;
    }

    @Override
    public boolean available() {
        return licenseState.isEqlAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        if (enabled) {
            EqlStatsRequest request = new EqlStatsRequest();
            request.includeStats(true);
            client.execute(EqlStatsAction.INSTANCE, request, ActionListener.wrap(r -> {
                List<Counters> countersPerNode = r.getNodes()
                        .stream()
                        .map(EqlStatsResponse.NodeStatsResponse::getStats)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                Counters mergedCounters = Counters.merge(countersPerNode);
                listener.onResponse(new EqlFeatureSetUsage(available(), enabled(), mergedCounters.toNestedMap()));
            }, listener::onFailure));
        } else {
            listener.onResponse(new EqlFeatureSetUsage(available(), enabled(), Collections.emptyMap()));
        }
    }

}
