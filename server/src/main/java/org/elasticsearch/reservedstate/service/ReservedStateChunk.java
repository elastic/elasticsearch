/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import java.util.Map;

/**
 * A holder for the cluster state to be saved and reserved and the version info
 * <p>
 * Apart from the cluster state we want to store and reserve, the chunk requires that
 * you supply the version metadata. This version metadata (see {@link ReservedStateVersion}) is checked to ensure
 * that the update is safe, and it's not unnecessarily repeated.
 */
public record ReservedStateChunk(Map<String, Object> state, ReservedStateVersion metadata) {}
