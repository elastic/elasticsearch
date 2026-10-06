/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.fetch.lifetime;

/**
 * A transport request that may look up the reader contexts of the fetch phase. A data node rejects every other request
 * that presents the id of such a context, so a search, scroll or point in time request can't read or extend it. The
 * search APIs that free a context by id don't look it up, so they can still free it, as they can any registered context.
 */
public interface FetchContextRequest {}
