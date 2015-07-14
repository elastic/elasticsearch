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

package org.elasticsearch.search.suggest.completion.context;

/**
 * Builder for {@link ContextMapping}
 */
public abstract class ContextBuilder<E extends ContextMapping> {

    protected String name;

    /**
     * @param name of the context mapper to build
     */
    protected ContextBuilder(String name) {
        this.name = name;
    }

    public abstract E build();

    /**
     * Create a new {@link GeoContextMapping}
     */
    public static GeoContextMapping.Builder geo(String name) {
        return new GeoContextMapping.Builder(name);
    }

    /**
     * Create a new {@link CategoryContextMapping}
     */
    public static CategoryContextMapping.Builder category(String name) {
        return new CategoryContextMapping.Builder(name);
    }

}
