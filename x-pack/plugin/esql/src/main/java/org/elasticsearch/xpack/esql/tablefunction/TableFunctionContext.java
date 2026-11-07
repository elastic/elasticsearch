/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;

/**
 * What every table function needs and no more: the project as the node doing the binding has applied it.
 * <p>
 * The same type serves the coordinator and an executing node, which is the point — a function derives its read
 * against whichever project it is handed, so the node's own state is what a node binds against rather than anything
 * the plan carried.
 * <p>
 * Nothing belonging to one function appears here. A function that needs more than the project is constructed with
 * it, and {@link TableFunctionRegistry#of} is where that happens.
 *
 * @param projectMetadata the project the query runs in, as this node has applied it
 */
public record TableFunctionContext(ProjectMetadata projectMetadata) {

    /** The named data source, or {@code null} when this project has none. */
    @Nullable
    public DataSource dataSource(String name) {
        return DataSourceMetadata.get(projectMetadata).get(name);
    }
}
