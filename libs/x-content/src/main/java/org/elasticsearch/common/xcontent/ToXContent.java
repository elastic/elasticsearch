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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.Booleans;

import java.io.IOException;
import java.util.Map;

/**
 * An interface allowing to transfer an object to "XContent" using an {@link XContentBuilder}.
 * The output may or may not be a value object. Objects implementing {@link ToXContentObject} output a valid value
 * but those that don't may or may not require emitting a startObject and an endObject.
 */
public interface ToXContent {

    interface Params {
        String param(String key);

        String param(String key, String defaultValue);

        boolean paramAsBoolean(String key, boolean defaultValue);

        Boolean paramAsBoolean(String key, Boolean defaultValue);
    }

    Params EMPTY_PARAMS = new Params() {
        @Override
        public String param(String key) {
            return null;
        }

        @Override
        public String param(String key, String defaultValue) {
            return defaultValue;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            return defaultValue;
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            return defaultValue;
        }

    };

    class MapParams implements Params {

        private final Map<String, String> params;

        public MapParams(Map<String, String> params) {
            this.params = params;
        }

        @Override
        public String param(String key) {
            return params.get(key);
        }

        @Override
        public String param(String key, String defaultValue) {
            String value = params.get(key);
            if (value == null) {
                return defaultValue;
            }
            return value;
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            return Booleans.parseBoolean(param(key), defaultValue);
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            return Booleans.parseBoolean(param(key), defaultValue);
        }
    }

    class DelegatingMapParams extends MapParams {

        private final Params delegate;

        public DelegatingMapParams(Map<String, String> params, Params delegate) {
            super(params);
            this.delegate = delegate;
        }

        @Override
        public String param(String key) {
            return super.param(key, delegate.param(key));
        }

        @Override
        public String param(String key, String defaultValue) {
            return super.param(key, delegate.param(key, defaultValue));
        }

        @Override
        public boolean paramAsBoolean(String key, boolean defaultValue) {
            return super.paramAsBoolean(key, delegate.paramAsBoolean(key, defaultValue));
        }

        @Override
        public Boolean paramAsBoolean(String key, Boolean defaultValue) {
            return super.paramAsBoolean(key, delegate.paramAsBoolean(key, defaultValue));
        }
    }

    XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException;

    default boolean isFragment() {
        return true;
    }

}
