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

import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;

public interface WriteRequestBuilder<B extends WriteRequestBuilder<B>> {
    WriteRequest<?> request();

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}, the default).
     */
    @SuppressWarnings("unchecked")
    default B setRefreshPolicy(RefreshPolicy refreshPolicy) {
        request().setRefreshPolicy(refreshPolicy);
        return (B) this;
    }

    /**
     * If set to true then this request will force an immediate refresh. Backwards compatibility layer for Elasticsearch's old
     * {@code setRefresh} calls.
     */
    // NOCOMMIT deprecate or just remove this
    @SuppressWarnings("unchecked")
    default B setRefresh(boolean refresh) {
        request().setRefreshPolicy(refresh ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE);
        return (B) this;
    }
}
