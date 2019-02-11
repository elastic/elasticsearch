/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;

import java.util.Collection;
import java.util.Collections;

/**
 * Factory for index checks
 */
public interface IndexUpgradeCheckFactory {

    /**
     * Using this method the check can expose additional user parameter that can be specified by the user on upgrade api
     *
     * @return the list of supported parameters
     */
    default Collection<String> supportedParams() {
        return Collections.emptyList();
    }

    /**
     * Creates an upgrade check
     * <p>
     * This method is called from {@link org.elasticsearch.plugins.Plugin#createComponents} method.
     */
    IndexUpgradeCheck createCheck(Client client, ClusterService clusterService);

}
