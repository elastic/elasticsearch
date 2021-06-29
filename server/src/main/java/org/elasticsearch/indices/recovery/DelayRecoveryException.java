/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * An exception marking that this recovery attempt should be ignored (since probably, we already recovered).
 *
 *
 */
public class DelayRecoveryException extends ElasticsearchException {

    public DelayRecoveryException(String msg) {
        super(msg);
    }

    public DelayRecoveryException(StreamInput in) throws IOException{
        super(in);
    }

}
