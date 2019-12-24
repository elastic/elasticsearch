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

package org.elasticsearch.action.support;

import org.elasticsearch.common.CheckedConsumer;

public class PlainActionFuture<T> extends AdapterActionFuture<T, T> {

    public static <T> PlainActionFuture<T> newFuture() {
        return new PlainActionFuture<>();
    }

    public static <T, E extends Exception> T get(CheckedConsumer<PlainActionFuture<T>, E> e) throws E {
        PlainActionFuture<T> fut = newFuture();
        e.accept(fut);
        return fut.actionGet();
    }

    @Override
    protected T convert(T listenerResponse) {
        return listenerResponse;
    }
}
