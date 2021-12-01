/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.test.rest;

import org.gradle.api.Action;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;

import javax.inject.Inject;

/**
 * Custom extension to configure the {@link CopyRestApiTask}
 */
public class RestResourcesExtension {

    private final RestResourcesSpec restApi;
    private final XpackRestResourcesSpec restTests;

    @Inject
    public RestResourcesExtension(ObjectFactory objects) {
        restApi = new RestResourcesSpec(objects);
        restTests = new XpackRestResourcesSpec(objects);
    }

    void restApi(Action<? super RestResourcesSpec> spec) {
        spec.execute(restApi);
    }

    void restTests(Action<? super XpackRestResourcesSpec> spec) {
        spec.execute(restTests);
    }

    public RestResourcesSpec getRestApi() {
        return restApi;
    }

    public XpackRestResourcesSpec getRestTests() {
        return restTests;
    }

    public static class RestResourcesSpec {

        private final ListProperty<String> include;

        RestResourcesSpec(ObjectFactory objects) {
            include = objects.listProperty(String.class);
        }

        public void include(String... include) {
            this.include.addAll(include);
        }

        public ListProperty<String> getInclude() {
            return include;
        }
    }

    public static class XpackRestResourcesSpec {

        private final ListProperty<String> includeCore;
        private final ListProperty<String> includeXpack;

        XpackRestResourcesSpec(ObjectFactory objects) {
            includeCore = objects.listProperty(String.class);
            includeXpack = objects.listProperty(String.class);
        }

        public void includeCore(String... include) {
            this.includeCore.addAll(include);
        }

        public void includeXpack(String... include) {
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
