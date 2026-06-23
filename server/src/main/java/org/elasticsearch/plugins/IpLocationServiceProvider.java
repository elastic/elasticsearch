/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.env.Environment;
import org.elasticsearch.iplocation.api.IpLocationService;

import java.util.function.Consumer;

/**
 * SPI interface for plugins that provide an {@link IpLocationService}.
 * Implemented by the ip-location module plugin; used by
 * {@link org.elasticsearch.node.NodeConstruction} to obtain the service
 * before {@link org.elasticsearch.ingest.IngestService} construction.
 */
public interface IpLocationServiceProvider {

    /**
     * Creates an {@link IpLocationService} for the given environment.
     */
    IpLocationService createIpLocationService(
        Environment environment,
        Client client,
        Consumer<Runnable> genericExecutor,
        ClusterService clusterService,
        ProjectResolver projectResolver
    );
}
