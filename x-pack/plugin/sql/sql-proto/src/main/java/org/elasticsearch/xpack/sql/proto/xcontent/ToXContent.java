/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.xcontent;

import org.elasticsearch.xpack.sql.proto.core.Booleans;

import java.io.IOException;
import java.util.Map;

/**
 * NB: Light-clone from XContent library to keep JDBC driver independent.
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
