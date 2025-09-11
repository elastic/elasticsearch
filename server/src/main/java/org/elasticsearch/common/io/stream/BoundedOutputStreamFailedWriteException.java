/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.ElasticsearchException;

/**
 * An exception indicating we have tried to write to the BoundedOutputStream and have exceeded capacity
 */
public class BoundedOutputStreamFailedWriteException extends ElasticsearchException {
    public BoundedOutputStreamFailedWriteException() {
        super("The write failed because there is no more capacity inside the BoundedOutputStream");
    }
}
