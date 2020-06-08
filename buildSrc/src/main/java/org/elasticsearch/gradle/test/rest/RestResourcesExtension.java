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
    private String sourceSetName = "yamlRestTest";

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

    public void sourceSetName(String sourceSetName) {
        this.sourceSetName = sourceSetName;
    }

    public String getSourceSetName() {
        return this.sourceSetName;
    }

    static class RestResourcesSpec {

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
