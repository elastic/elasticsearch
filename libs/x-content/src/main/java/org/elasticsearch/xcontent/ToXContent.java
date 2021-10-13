/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.core.Booleans;

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
