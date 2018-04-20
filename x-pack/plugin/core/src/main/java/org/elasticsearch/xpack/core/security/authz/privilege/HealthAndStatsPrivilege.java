/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authz.privilege;

public final class HealthAndStatsPrivilege extends Privilege {

    public static final HealthAndStatsPrivilege INSTANCE = new HealthAndStatsPrivilege();

    public static final String NAME = "health_and_stats";

    private HealthAndStatsPrivilege() {
        super(NAME, "cluster:monitor/health*",
                    "cluster:monitor/stats*",
                    "indices:monitor/stats*",
                    "cluster:monitor/nodes/stats*");
    }
}
