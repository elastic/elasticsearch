/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.test.rest;

import org.elasticsearch.gradle.info.BuildParams;
import org.gradle.api.Action;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;

import javax.inject.Inject;

/**
 * Custom extension to configure the {@link CopyRestApiTask}
 */
public class RestResourcesExtension {

    final RestResourcesSpec restApi;
    final RestResourcesSpec restTests;

    @Inject
    public RestResourcesExtension(ObjectFactory objects) {
        restApi = new RestResourcesSpec(objects);
        restTests = new RestResourcesSpec(objects);
    }

    void restApi(Action<? super RestResourcesSpec> spec) {
        spec.execute(restApi);
    }

    void restTests(Action<? super RestResourcesSpec> spec) {
        spec.execute(restTests);
    }

    public RestResourcesSpec getRestApi() {
        return restApi;
    }

    public RestResourcesSpec getRestTests() {
        return restTests;
    }

    public static class RestResourcesSpec {

        private final ListProperty<String> includeCore;
        private final ListProperty<String> includeXpack;

        RestResourcesSpec(ObjectFactory objects) {
            includeCore = objects.listProperty(String.class);
            includeXpack = objects.listProperty(String.class);
        }

        public void includeCore(String... include) {
            this.includeCore.addAll(include);
        }

        public void includeXpack(String... include) {
            if (BuildParams.isInternal() == false) {
                throw new IllegalStateException("Can not include x-pack rest resources from an external build.");
            }
            this.includeXpack.addAll(include);
        }

        public ListProperty<String> getIncludeCore() {
            return includeCore;
        }

        public ListProperty<String> getIncludeXpack() {
            return includeXpack;
        }
    }
}
