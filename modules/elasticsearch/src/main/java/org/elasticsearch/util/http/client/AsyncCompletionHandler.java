/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */
package org.elasticsearch.util.http.client;

import org.elasticsearch.util.logging.ESLogger;
import org.elasticsearch.util.logging.Loggers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * An {@link AsyncHandler} augmented with an {@link #onCompleted(Response)} convenience method which gets called
 * when the {@link Response} has been fully received.
 *
 * @param <T>  Type of the value that will be returned by the associated {@link java.util.concurrent.Future}
 */
public abstract class AsyncCompletionHandler<T> implements AsyncHandler<T> {

    private final static ESLogger log = Loggers.getLogger(AsyncCompletionHandlerBase.class);

    private final Collection<HttpResponseBodyPart<T>> bodies =
            Collections.synchronizedCollection(new ArrayList<HttpResponseBodyPart<T>>());
    private HttpResponseStatus<T> status;
    private HttpResponseHeaders<T> headers;

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public STATE onBodyPartReceived(final HttpResponseBodyPart<T> content) throws Exception {
        bodies.add(content);
        return STATE.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public final STATE onStatusReceived(final HttpResponseStatus<T> status) throws Exception {
        this.status = status;
        return STATE.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public final STATE onHeadersReceived(final HttpResponseHeaders<T> headers) throws Exception {
        this.headers = headers;
        return STATE.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public final T onCompleted() throws Exception {
        return onCompleted(status == null ? null : status.provider().prepareResponse(status, headers, bodies));
    }

    /**
     * {@inheritDoc}
     */
    /* @Override */
    public void onThrowable(Throwable t) {
        if (log.isDebugEnabled())
            log.debug(t.getMessage(), t);
    }

    /**
     * Invoked once the HTTP response has been fully read.
     *
     * @param response The {@link Response}
     * @return Type of the value that will be returned by the associated {@link java.util.concurrent.Future}
     */
    abstract public T onCompleted(Response response) throws Exception;
}
