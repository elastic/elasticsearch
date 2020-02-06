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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.CheckedConsumer;

import java.util.function.Consumer;

// public for testing
public interface RejectAwareActionListener<T> extends ActionListener<T> {
    void onRejection(Exception e);

    /**
     * Return a new listener that delegates failure/reject to errorDelegate but forwards response to responseHandler
     */
    static <X> RejectAwareActionListener<X> withResponseHandler(RejectAwareActionListener<?> errorDelegate, Consumer<X> responseHandler) {
        return new RejectAwareActionListener<>() {
            @Override
            public void onRejection(Exception e) {
                errorDelegate.onRejection(e);
            }

            @Override
            public void onResponse(X t) {
                responseHandler.accept(t);
            }

            @Override
            public void onFailure(Exception e) {
                errorDelegate.onFailure(e);
            }
        };
    }

    /**
     * Similar to {@link ActionListener#wrap(CheckedConsumer, Consumer)}, extended to have handler for onRejection.
     */
    static <Response> RejectAwareActionListener<Response> wrap(CheckedConsumer<Response, ? extends Exception> onResponse,
                                                    Consumer<Exception> onFailure, Consumer<Exception> onRejection) {
        return new RejectAwareActionListener<Response>() {
            @Override
            public void onResponse(Response response) {
                try {
                    onResponse.accept(response);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                onFailure.accept(e);
            }

            @Override
            public void onRejection(Exception e) {
                onRejection.accept(e);
            }
        };
    }

}

