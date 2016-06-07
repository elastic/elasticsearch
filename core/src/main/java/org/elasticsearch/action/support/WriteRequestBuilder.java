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

import org.elasticsearch.Version;
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
     *
     * @deprecated use {@link #setRefreshPolicy(RefreshPolicy)} with {@link RefreshPolicy#IMMEDIATE} or {@link RefreshPolicy#NONE} instead.
     *             Will be removed in 6.0.
     */
    @Deprecated
    default B setRefresh(boolean refresh) {
        assert Version.CURRENT.major < 6 : "Remove setRefresh(boolean) in 6.0";
        return setRefreshPolicy(refresh ? RefreshPolicy.IMMEDIATE : RefreshPolicy.NONE);
    }
}
