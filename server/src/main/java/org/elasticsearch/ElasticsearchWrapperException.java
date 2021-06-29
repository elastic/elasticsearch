/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

/**
 * An exception that is meant to be "unwrapped" when sent back to the user
 * as an error because its is {@link #getCause() cause}, if non-null is
 * <strong>always</strong> more useful to the user than the exception itself.
 */
public interface ElasticsearchWrapperException {
    Throwable getCause();
}
