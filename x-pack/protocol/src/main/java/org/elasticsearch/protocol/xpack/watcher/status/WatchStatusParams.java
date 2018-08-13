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
package org.elasticsearch.protocol.xpack.watcher.status;

import org.elasticsearch.common.xcontent.ToXContent;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class WatchStatusParams extends ToXContent.DelegatingMapParams {
    private static final String HIDE_HEADERS = "hide_headers";
    private static final String INCLUDE_STATE = "include_state";

    public WatchStatusParams(Map<String, String> params, ToXContent.Params delegate) {
        super(params, delegate);
    }

    public static WatchStatusParams.Builder builder() {
        return new Builder(ToXContent.EMPTY_PARAMS);
    }

    public static boolean hideHeaders(ToXContent.Params params) {
        return params.paramAsBoolean(HIDE_HEADERS, true);
    }

    public static boolean includeState(ToXContent.Params params) {
        return params.paramAsBoolean(INCLUDE_STATE, true);
    }

    public static class Builder {
        private final ToXContent.Params delegate;
        private final Map<String, String> params = new HashMap<>();

        private Builder(ToXContent.Params delegate) {
            this.delegate = delegate;
        }

        public Builder hideHeaders(boolean hideHeaders) {
            params.put(HIDE_HEADERS, String.valueOf(hideHeaders));
            return this;
        }

        public Builder includeState(boolean includeState) {
            params.put(INCLUDE_STATE, String.valueOf(includeState));
            return this;
        }

        public WatchStatusParams build() {
            return new WatchStatusParams(unmodifiableMap(new HashMap<>(params)), delegate);
        }
    }

}
