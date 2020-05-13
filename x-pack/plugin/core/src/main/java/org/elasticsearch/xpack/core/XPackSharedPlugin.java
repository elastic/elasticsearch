/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Supplier;

public interface XPackSharedPlugin {
    default Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                                ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                                NamedXContentRegistry xContentRegistry, Environment environment,
                                                NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry,
                                                IndexNameExpressionResolver indexNameExpressionResolver,
                                                Supplier<RepositoriesService> repositoriesServiceSupplier,
                                                SSLService sslService,
                                                LicenseService licenseService,
                                                XPackLicenseState licenseState) {
        return Collections.emptyList();
    }
}
