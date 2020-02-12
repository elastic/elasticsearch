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

import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;

public class RestResourcesSpec {

    private final ListProperty<String> includeCore;
    private final ListProperty<String> includeXpack;

    public RestResourcesSpec(ObjectFactory objects) {
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
