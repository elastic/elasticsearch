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

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;

/**
 * A filter allowing to filter transport actions
 */
public interface ActionFilter {

    /**
     * The position of the filter in the chain. Execution is done from lowest order to highest.
     */
    int order();

    /**
     * Enables filtering the execution of an action on the request side, either by sending a response through the
     * {@link ActionListener} or by continuing the execution through the given {@link ActionFilterChain chain}
     */
    <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain);
    /**
     * A simple base class for injectable action filters that spares the implementation from handling the
     * filter chain. This base class should serve any action filter implementations that doesn't require
     * to apply async filtering logic.
     */
    abstract class Simple extends AbstractComponent implements ActionFilter {

        protected Simple(Settings settings) {
            super(settings);
        }

        @Override
        public final <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request,
                ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
            if (apply(action, request, listener)) {
                chain.proceed(task, action, request, listener);
            }
        }

        /**
         * Applies this filter and returns {@code true} if the execution chain should proceed, or {@code false}
         * if it should be aborted since the filter already handled the request and called the given listener.
         */
        protected abstract boolean apply(String action, ActionRequest request, ActionListener<?> listener);
    }
}
