/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.user;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Optional;

public class FakeActionUserContext extends ActionUserContext {

    public FakeActionUserContext(ThreadContext threadContext) {
        super(tc -> Optional.empty(), threadContext);
    }

    public FakeActionUserContext(ThreadPool threadPool) {
        this(threadPool.getThreadContext());
    }
}
