/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate;

import java.util.Set;

/**
 * A wrapper class for notifying listeners on non cluster state transformation operation completion.
 * <p>
 * Certain {@link ReservedClusterStateHandler} implementations may need to perform additional
 * operations other than modifying the cluster state. This can range from cache
 * invalidation to implementing state handlers that do not write to the cluster state, e.g. role mappings.
 * These additional transformation steps are implemented as separate async operation after the validation of
 * the cluster state update steps (trial run in {@link org.elasticsearch.reservedstate.service.ReservedClusterStateService}).
 */
public record NonStateTransformResult(String handlerName, Set<String> updatedKeys) {}
