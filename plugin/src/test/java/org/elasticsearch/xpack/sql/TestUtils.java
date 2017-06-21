/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.sql.analysis.catalog.EsCatalog;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;

public class TestUtils {
    // NOCOMMIT I think these may not be needed if we switch to integration tests for the protos
    public static PlanExecutor planExecutor(Client client) {
        PlanExecutor executor = new PlanExecutor(client,
                () -> client.admin().cluster().prepareState().get(TimeValue.timeValueMinutes(1)).getState());
        ((EsCatalog) executor.catalog()).setIndexNameExpressionResolver(new IndexNameExpressionResolver(client.settings()));
        return executor;
    }
}