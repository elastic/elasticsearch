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

package org.elasticsearch.transport.nio.channel;

import org.elasticsearch.action.support.PlainListenableActionFuture;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CloseFuture extends PlainListenableActionFuture<NioChannel> {

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException("Cannot cancel close future");
    }

    public void awaitClose() throws IOException {
        try {
            super.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause();
        }
    }

    public void awaitClose(long timeout, TimeUnit unit) throws TimeoutException, IOException {
        try {
            super.get(timeout, unit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Future got interrupted", e);
        } catch (ExecutionException e) {
            throw (IOException) e.getCause();
        }
    }

    public IOException getCloseException() {
        if (isDone()) {
            try {
                super.get(0, TimeUnit.NANOSECONDS);
                return null;
            } catch (ExecutionException e) {
                // We only make a setter for IOException
                return (IOException) e.getCause();
            } catch (TimeoutException e) {
                return null;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        } else {
            return null;
        }
    }

    public boolean isClosed() {
        return super.isDone();
    }

    boolean channelClosed(NioChannel channel) {
        return set(channel);
    }


    boolean channelCloseThrewException(IOException ex) {
        return setException(ex);
    }

}
