/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.exception;

import org.apache.lucene.index.CorruptIndexException;

public class ChecksumCombinationException extends CorruptIndexException {
    public ChecksumCombinationException(String msg, String resourceDescription) {
        super(msg, resourceDescription);
    }

    public ChecksumCombinationException(String msg, String resourceDescription, Throwable cause) {
        super(msg, resourceDescription, cause);
    }
}
