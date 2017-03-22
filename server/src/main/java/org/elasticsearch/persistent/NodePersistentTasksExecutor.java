/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.persistent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse.Empty;

/**
 * This component is responsible for execution of persistent tasks.
 */
public class NodePersistentTasksExecutor {
    private final ThreadPool threadPool;

    public NodePersistentTasksExecutor(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public <Request extends PersistentTaskRequest> void executeTask(Request request,
                                                                    NodePersistentTask task,
                                                                    PersistentTasksExecutor<Request> action,
                                                                    ActionListener<Empty> listener) {
        threadPool.executor(action.getExecutor()).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }

            @SuppressWarnings("unchecked")
            @Override
            protected void doRun() throws Exception {
                try {
                    action.nodeOperation(task, request, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }

            }
        });

    }

}
