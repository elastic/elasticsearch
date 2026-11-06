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
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;

import java.util.Map;

/**
 * What a table function needs from the coordinator to bind a call: this coordinator's view of the project, and the
 * per-type validators the registration API validates with.
 * <p>
 * The validators are here because they are plugin-contributed and the query path has no other handle on them — the
 * registration services each hold their own copy, and {@code DatasetRewriter} says in as many words that it cannot
 * reach them. Passing them in is what lets an inline definition meet the same bar a registered one does instead of a
 * second, drifting approximation of it.
 *
 * @param projectMetadata the project the query runs in, as this coordinator has applied it
 * @param validatorsByType data source type to its validator, keyed as {@code DataSource#type} reports it
 */
public record TableFunctionContext(ProjectMetadata projectMetadata, Map<String, DataSourceValidator> validatorsByType) {

    /** The named data source, or {@code null} when this project has none. */
    @Nullable
    public DataSource dataSource(String name) {
        return DataSourceMetadata.get(projectMetadata).get(name);
    }

    /** The validator for a data source type, or {@code null} when no plugin registered one. */
    @Nullable
    public DataSourceValidator validator(String type) {
        return validatorsByType.get(type);
    }
}
