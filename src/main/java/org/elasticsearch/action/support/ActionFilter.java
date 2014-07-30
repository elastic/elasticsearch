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

/**
 * A filter allowing to filter transport actions
 */
public interface ActionFilter {

    /**
     * Filters the actual execution of the request by either sending a response through the {@link ActionListener}
     * or continuing the filters execution through the {@link ActionFilterChain}
     */
    void process(final String action, final ActionRequest actionRequest, final ActionListener actionListener, final ActionFilterChain actionFilterChain);

    /**
     * The position of the filter in the chain. Execution is done from lowest order to highest.
     */
    int order();
}
