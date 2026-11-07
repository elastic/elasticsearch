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
 *
 * @param projectMetadata the project the query runs in, as this node has applied it
 */
public record TableFunctionContext(ProjectMetadata projectMetadata) {}
