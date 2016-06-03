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

package org.elasticsearch.transport;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Base class for delegating transport response to a transport channel
 */
public class TransportChannelResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

    private final ESLogger logger;
    private final TransportChannel channel;
    private final String extraInfoOnError;
    private final Supplier<T> responseSupplier;

    public TransportChannelResponseHandler(ESLogger logger, TransportChannel channel, String extraInfoOnError,
                                           Supplier<T> responseSupplier) {
        this.logger = logger;
        this.channel = channel;
        this.extraInfoOnError = extraInfoOnError;
        this.responseSupplier = responseSupplier;
    }

    @Override
    public T newInstance() {
        return responseSupplier.get();
    }

    @Override
    public void handleResponse(T response) {
        try {
            channel.sendResponse(response);
        } catch (IOException e) {
            handleException(new TransportException(e));
        }
    }

    @Override
    public void handleException(TransportException exp) {
        try {
            channel.sendResponse(exp);
        } catch (IOException e) {
            logger.debug("failed to send failure {}", e, extraInfoOnError == null ? "" : "(" + extraInfoOnError + ")");
        }
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SAME;
    }
}
