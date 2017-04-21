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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ConnectFuture extends BaseFuture<NioSocketChannel> {

    public boolean awaitConnectionComplete(long timeout, TimeUnit unit) throws InterruptedException {
        try {
            super.get(timeout, unit);
            return true;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        }
    }

    public NioSocketChannel getChannel() {
        if (isDone()) {
            try {
                return super.get(0, TimeUnit.NANOSECONDS);
            } catch (InterruptedException | TimeoutException | ExecutionException e) {
                return null;
            }
        } else {
            return null;
        }
    }

    public Exception getException() {
        if (isDone()) {
            try {
                super.get(0, TimeUnit.NANOSECONDS);
                return null;
            } catch (ExecutionException e) {
                // We only make a public setters for IOException or RuntimeException
                return (Exception) e.getCause();
            } catch (InterruptedException | TimeoutException e) {
                return null;
            }
        } else {
            return null;
        }
    }

    public boolean isConnectComplete() {
        return getChannel() != null;
    }

    public boolean connectFailed() {
        return getException() != null;
    }

    public boolean setConnectionComplete(NioSocketChannel channel) {
        return set(channel);
    }

    public boolean setConnectionFailed(IOException e) {
        return setException(e);
    }

    public boolean setConnectionFailed(RuntimeException e) {
        return setException(e);
    }
}
