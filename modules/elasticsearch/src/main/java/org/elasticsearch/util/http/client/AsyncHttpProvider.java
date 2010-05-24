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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.Future;

/**
 * Interface to be used when implementing custom asynchronous I/O HTTP client.
 * By default, the {@link org.elasticsearch.util.http.client.providers.NettyAsyncHttpProvider} is used.
 */
public interface AsyncHttpProvider<A> {

    /**
     * Execute the request and invoke the {@link AsyncHandler} when the response arrive.
     *
     * @param handler an instance of {@link AsyncHandler}
     * @return a {@link java.util.concurrent.Future} of Type T.
     * @throws IOException
     */
    public <T> Future<T> execute(Request request, AsyncHandler<T> handler) throws IOException;

    /**
     * Close the current underlying TCP/HTTP connection.s
     */
    public void close();

    /**
     * Prepare a {@link Response}
     *
     * @param status    {@link HttpResponseStatus}
     * @param headers   {@link HttpResponseHeaders}
     * @param bodyParts list of {@link HttpResponseBodyPart}
     * @return a {@link Response}
     */
    public Response prepareResponse(HttpResponseStatus<A> status,
                                    HttpResponseHeaders<A> headers,
                                    Collection<HttpResponseBodyPart<A>> bodyParts);

}
