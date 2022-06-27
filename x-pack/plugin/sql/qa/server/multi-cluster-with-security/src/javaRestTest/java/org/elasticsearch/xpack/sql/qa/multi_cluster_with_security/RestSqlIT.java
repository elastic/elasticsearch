/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.qa.multi_cluster_with_security;

import org.elasticsearch.xpack.sql.qa.rest.RestSqlTestCase;

import static org.elasticsearch.transport.RemoteClusterAware.buildRemoteIndexName;

public class RestSqlIT extends RestSqlTestCase {

    public static final String REMOTE_CLUSTER_NAME = "my_remote_cluster"; // gradle defined

    @Override
    protected String indexPattern(String pattern) {
        if (randomBoolean()) {
            return buildRemoteIndexName(REMOTE_CLUSTER_NAME, pattern);
        } else {
            String cluster = REMOTE_CLUSTER_NAME.substring(0, randomIntBetween(0, REMOTE_CLUSTER_NAME.length())) + "*";
            if (pattern.startsWith("\\\"") && pattern.endsWith("\\\"") && pattern.length() > 4) {
                pattern = pattern.substring(2, pattern.length() - 2);
            }
            return "\\\"" + buildRemoteIndexName(cluster, pattern) + "\\\""; // rest tests don't do JSON escaping
        }
    }
}
