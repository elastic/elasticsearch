/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.cluster.metadata.ProjectMetadata;

/**
 * What every table function gets, and no more: the project as this node has applied it.
 * <p>
 * Nothing belonging to one function appears here, and nothing shared carries one function's dependencies. A
 * function that needs more than the project is constructed with it, which is why the registry is built from a
 * list of already-constructed functions rather than from names.
 * <p>
 * It stays a record of one component on purpose. The pull to widen it is constant — each new function wants one
 * more collaborator — and every widening is paid by every function that does not need it.
 *
 * @param projectMetadata the project the query runs in, as this node has applied it
 */
public record TableFunctionContext(ProjectMetadata projectMetadata) {}
