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
 */
package org.elasticsearch.util.http.client;

/**
 * An asynchronous handler or callback which gets invoked as soon as some data are available when
 * processing an asynchronous response. Callbacks method gets invoked in the following order:
 * (1) {@link #onStatusReceived(HttpResponseStatus)}
 * (2) {@link #onHeadersReceived(HttpResponseHeaders)}
 * (3) {@link #onBodyPartReceived(HttpResponseBodyPart)}, which could be invoked multiple times
 * (4) {@link #onCompleted()}, once the response has been fully read.
 *
 * Interrupting the process of the asynchronous response can be achieved by
 * returning a {@link AsyncHandler.STATE#ABORT} at any moment during the
 * processing of the asynchronous response.
 *
 * @param <T> Type of object returned by the {@link java.util.concurrent.Future#get}
 */
public interface AsyncHandler<T> {

    public static enum STATE {
        ABORT, CONTINUE
    }

    /**
     * Invoked when an unexpected exception occurs during the processing of the response
     *
     * @param t a {@link Throwable}
     */
    void onThrowable(Throwable t);

    /**
     * Invoked as soon as some response body part are received.
     *
     * @param bodyPart response's body part.
     * @throws Exception
     */
    STATE onBodyPartReceived(HttpResponseBodyPart<T> bodyPart) throws Exception;

    /**
     * Invoked as soon as the HTTP status line has been received
     *
     * @param responseStatus the status code and test of the response
     * @throws Exception
     */
    STATE onStatusReceived(HttpResponseStatus<T> responseStatus) throws Exception;

    /**
     * Invoked as soon as the HTTP headers has been received.
     *
     * @param headers the HTTP headers.
     * @throws Exception
     */
    STATE onHeadersReceived(HttpResponseHeaders<T> headers) throws Exception;

    /**
     * Invoked once the HTTP response has been fully received
     *
     * @return T Type of the value that will be returned by the associated {@link java.util.concurrent.Future}
     * @throws Exception
     */
    T onCompleted() throws Exception;
}
