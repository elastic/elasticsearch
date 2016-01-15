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

/**
 * Base class for delegating transport response to a transport channel
 */
public abstract class TransportChannelResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

    /**
     * Convenience method for delegating an empty response to the provided changed
     */
    public static TransportChannelResponseHandler<TransportResponse.Empty> emptyResponseHandler(ESLogger logger, TransportChannel channel, String extraInfoOnError) {
        return new TransportChannelResponseHandler<TransportResponse.Empty>(logger, channel, extraInfoOnError) {
            @Override
            public TransportResponse.Empty newInstance() {
                return TransportResponse.Empty.INSTANCE;
            }
        };
    }

    private final ESLogger logger;
    private final TransportChannel channel;
    private final String extraInfoOnError;

    protected TransportChannelResponseHandler(ESLogger logger, TransportChannel channel, String extraInfoOnError) {
        this.logger = logger;
        this.channel = channel;
        this.extraInfoOnError = extraInfoOnError;
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
