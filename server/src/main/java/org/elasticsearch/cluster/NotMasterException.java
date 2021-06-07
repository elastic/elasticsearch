/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;


import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown when a node join request or a master ping reaches a node which is not
 * currently acting as a master or when a cluster state update task is to be executed
 * on a node that is no longer master.
 */
public class NotMasterException extends ElasticsearchException {

    public NotMasterException(String msg) {
        super(msg);
    }

    public NotMasterException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }
}
