/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.condition;

import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;

public interface ExecutableCondition extends Condition {

    /**
     * Executes this condition
     */
    Result execute(WatchExecutionContext ctx);
}
