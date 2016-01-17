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

package org.elasticsearch.action;

/**
 * An ActionListener that does nothing. Used when we need a listener but don't
 * care to listen for the result.
 */
public final class NoopActionListener<Response> implements ActionListener<Response> {
    /**
     * Get the instance of NoopActionListener cast appropriately.
     */
    @SuppressWarnings("unchecked") // Safe because we do nothing with the type.
    public static <Response> ActionListener<Response> instance() {
        return (ActionListener<Response>) INSTANCE;
    }

    private static final NoopActionListener<Object> INSTANCE = new NoopActionListener<Object>();

    private NoopActionListener() {
    }

    @Override
    public void onResponse(Response response) {
    }

    @Override
    public void onFailure(Throwable e) {
    }
}
