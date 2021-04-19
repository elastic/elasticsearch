/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A generic exception that can be thrown by a task when it's cancelled by the task manager API
 */
public class TaskCancelledException  extends ElasticsearchException {

    public TaskCancelledException(String msg) {
        super(msg);
    }

    public TaskCancelledException(StreamInput in) throws IOException{
        super(in);
    }
}
