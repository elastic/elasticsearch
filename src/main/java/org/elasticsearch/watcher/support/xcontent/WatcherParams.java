/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.xcontent;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.ToXContent;

/**
 *
 */
public class WatcherParams extends ToXContent.DelegatingMapParams {

    public static final WatcherParams HIDE_SECRETS = WatcherParams.builder().hideSecrets(true).build();

    static final String HIDE_SECRETS_KEY = "hide_secrets";
    static final String COLLAPSE_ARRAYS_KEY = "collapse_arrays";

    private ImmutableMap<String, String> params;

    private WatcherParams(ImmutableMap<String, String> params, ToXContent.Params delegate) {
        super(params, delegate);
    }

    public boolean hideSecrets() {
        return paramAsBoolean(HIDE_SECRETS_KEY, false);
    }

    public boolean collapseArrays() {
        return paramAsBoolean(COLLAPSE_ARRAYS_KEY, false);
    }

    public static WatcherParams wrap(ToXContent.Params params) {
        return params instanceof WatcherParams ?
                (WatcherParams) params :
                new WatcherParams(ImmutableMap.<String, String>of(), params);
    }

    public static boolean hideSecrets(ToXContent.Params params) {
        return wrap(params).hideSecrets();
    }

    public static boolean collapseArrays(ToXContent.Params params) {
        return wrap(params).collapseArrays();
    }

    public static Builder builder() {
        return builder(ToXContent.EMPTY_PARAMS);
    }

    public static Builder builder(ToXContent.Params delegate) {
        return new Builder(delegate);
    }

    public static class Builder {

        private final ToXContent.Params delegate;
        private final ImmutableMap.Builder<String, String> params = ImmutableMap.builder();

        private Builder(ToXContent.Params delegate) {
            this.delegate = delegate;
        }

        public Builder hideSecrets(boolean hideSecrets) {
            params.put(HIDE_SECRETS_KEY, String.valueOf(hideSecrets));
            return this;
        }

        public Builder collapseArrays(boolean collapseArrays) {
            params.put(COLLAPSE_ARRAYS_KEY, String.valueOf(collapseArrays));
            return this;
        }

        public WatcherParams build() {
            return new WatcherParams(params.build(), delegate);
        }
    }
}
