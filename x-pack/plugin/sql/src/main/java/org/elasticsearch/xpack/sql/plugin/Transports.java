/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.security.SecurityContext;

final class Transports {

    private Transports() {}
    
    static String username(SecurityContext securityContext) {
        return securityContext != null && securityContext.getUser() != null ? securityContext.getUser().principal() : null;
    }
    
    static String clusterName(ClusterService clusterService) {
        return clusterService.getClusterName().value();
    }
}
