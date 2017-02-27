/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;

/**
 * This component is responsible for execution of persistent actions.
 */
public class PersistentActionExecutor {
    private final ThreadPool threadPool;

    public PersistentActionExecutor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public <Request extends PersistentActionRequest> void executeAction(Request request,
                                                                        NodePersistentTask task,
                                                                        PersistentActionRegistry.PersistentActionHolder<Request> holder,
                                                                        ActionListener<Empty> listener) {
        threadPool.executor(holder.getExecutor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @SuppressWarnings("unchecked")
            @Override
            protected void doRun() throws Exception {
                try {
                    holder.getPersistentAction().nodeOperation(task, request, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }

            }
        });

    }

}
