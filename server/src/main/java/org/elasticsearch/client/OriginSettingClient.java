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

package org.elasticsearch.client;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.function.Supplier;

/**
 * A {@linkplain Client} that sends requests with the
 * {@link ThreadContext#stashWithOrigin origin} set to a particular
 * value and calls its {@linkplain ActionListener} in its original
 * {@link ThreadContext}.
 */
public final class OriginSettingClient extends FilterClient {

    private final String origin;

    public OriginSettingClient(Client in, String origin) {
        super(in);
        this.origin = origin;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
        final Supplier<ThreadContext.StoredContext> supplier = in().threadPool().getThreadContext().newRestorableContext(false);
        try (ThreadContext.StoredContext ignore = in().threadPool().getThreadContext().stashWithOrigin(origin)) {
            super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }
}
