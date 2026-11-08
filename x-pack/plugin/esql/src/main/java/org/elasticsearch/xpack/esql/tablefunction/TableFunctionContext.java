/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.cluster.metadata.ProjectMetadata;

/**
 * What every table function gets. A function needing anything else is constructed with it, so that one
 * function's dependency is not paid for by every function that does not need it.
 * <p>
 * <b>This carries no principal.</b> The facility checks no privilege, so a function that reads cluster-state
 * facts out of the project — the names of datasets, of data sources, of indices — is reading them as the node and
 * not as the caller, and has to authorize the caller itself before it answers. {@link TableFunction} says how:
 * dispatch the action the equivalent REST endpoint dispatches. Nothing here enforces that, which is why it is
 * written down here, next to the handle that makes it possible to forget.
 *
 * @param projectMetadata the project the query runs in, as this node has applied it
 */
public record TableFunctionContext(ProjectMetadata projectMetadata) {}
