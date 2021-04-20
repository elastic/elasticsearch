/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Function;

public interface TransportResponseHandler<T extends TransportResponse> extends Writeable.Reader<T> {

    void handleResponse(T response);

    void handleException(TransportException exp);

    default String executor() {
        return ThreadPool.Names.SAME;
    }

    default <Q extends TransportResponse> TransportResponseHandler<Q> wrap(Function<Q, T> converter, Writeable.Reader<Q> reader) {
        final TransportResponseHandler<T> self = this;
        return new TransportResponseHandler<Q>() {
            @Override
            public void handleResponse(Q response) {
                self.handleResponse(converter.apply(response));
            }

            @Override
            public void handleException(TransportException exp) {
                self.handleException(exp);
            }

            @Override
            public String executor() {
                return self.executor();
            }

            @Override
            public Q read(StreamInput in) throws IOException {
                return reader.read(in);
            }
        };
    }

    /**
     * Implementations of {@link TransportResponseHandler} that handles the empty response {@link TransportResponse.Empty}.
     */
    abstract class Empty implements TransportResponseHandler<TransportResponse.Empty> {
        @Override
        public final TransportResponse.Empty read(StreamInput in) {
            return TransportResponse.Empty.INSTANCE;
        }
    }
}
