/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

/**
 * {@link RefCounted} which does nothing when all references are released. It is the responsibility of the caller
 * to run whatever release logic should be executed when {@link AbstractRefCounted#decRef()} returns true.
 */
public class SimpleRefCounted extends AbstractRefCounted {
    @Override
    protected void closeInternal() {}
}
