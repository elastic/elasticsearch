/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.index;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.LinkedProjectConfig;
import org.elasticsearch.transport.LinkedProjectConfigService;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArraySet;

public final class RemoteClusterResolver extends RemoteClusterAware {
    private final CopyOnWriteArraySet<String> clusters;

    public RemoteClusterResolver(Settings settings, LinkedProjectConfigService linkedProjectConfigService) {
        super(settings);
        clusters = new CopyOnWriteArraySet<>(
            linkedProjectConfigService.getInitialLinkedProjectConfigs().stream().map(LinkedProjectConfig::linkedProjectAlias).toList()
        );
        linkedProjectConfigService.register(this);
    }

    @Override
    public void updateLinkedProject(LinkedProjectConfig config) {
        clusters.add(config.linkedProjectAlias());
    }

    @Override
    public void remove(ProjectId originProjectId, ProjectId linkedProjectId, String linkedProjectAlias) {
        clusters.remove(linkedProjectAlias);
    }

    public Set<String> remoteClusters() {
        return new TreeSet<>(clusters);
    }
}
