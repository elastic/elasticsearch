/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate;

import java.util.Set;

/**
 * A state wrapper used by the ReservedClusterStateService to pass the
 * current state as well as previous keys set by an {@link ReservedClusterStateHandler} to each transform
 * step of the state update.
 *
 * @param <S> The type of state to update
 */
public record TransformState<S>(S state, Set<String> keys) {}
