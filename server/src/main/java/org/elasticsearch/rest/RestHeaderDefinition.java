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

package org.elasticsearch.rest;

/**
 * A definition for an http header that should be copied to the {@link org.elasticsearch.common.util.concurrent.ThreadContext} when
 * reading the request on the rest layer.
 */
public final class RestHeaderDefinition {
    private final String name;
    /**
     * This should be set to true only when the syntax of the value of the Header to copy is defined as a comma separated list of String
     * values.
     */
    private final boolean multiValueAllowed;

    public RestHeaderDefinition(String name, boolean multiValueAllowed) {
        this.name = name;
        this.multiValueAllowed = multiValueAllowed;
    }

    public String getName() {
        return name;
    }

    public boolean isMultiValueAllowed() {
        return multiValueAllowed;
    }
}
