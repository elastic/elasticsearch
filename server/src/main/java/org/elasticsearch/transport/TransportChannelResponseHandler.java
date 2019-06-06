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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 * Base class for delegating transport response to a transport channel
 */
public class TransportChannelResponseHandler<T extends TransportResponse> implements TransportResponseHandler<T> {

    private final Logger logger;
    private final TransportChannel channel;
    private final String extraInfoOnError;
    private final Writeable.Reader<T> reader;

    public TransportChannelResponseHandler(Logger logger, TransportChannel channel, String extraInfoOnError,
                                           Writeable.Reader<T> reader) {
        this.logger = logger;
        this.channel = channel;
        this.extraInfoOnError = extraInfoOnError;
        this.reader = reader;
    }

    @Override
    public T read(StreamInput in) throws IOException {
        return reader.read(in);
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
            logger.debug(() -> new ParameterizedMessage(
                        "failed to send failure {}", extraInfoOnError == null ? "" : "(" + extraInfoOnError + ")"), e);
        }
    }

    @Override
    public String executor() {
        return ThreadPool.Names.SAME;
    }
}
