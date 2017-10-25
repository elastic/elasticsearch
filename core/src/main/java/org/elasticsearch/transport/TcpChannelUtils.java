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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class TcpChannelUtils {

    public static <C extends TcpChannel<C>> void closeChannels(List<C> channels, boolean blocking, Logger logger) {
        ArrayList<ListenableActionFuture<C>> futures = new ArrayList<>(channels.size());
        for (final C channel : channels) {
            if (channel != null && channel.isOpen()) {
                ListenableActionFuture<C> f = channel.closeAsync();
                f.addListener(ActionListener.wrap(c -> {},
                    e -> logger.debug(() -> new ParameterizedMessage("exception while closing channel: {}", channel), e)));
                futures.add(f);
            }
        }

        if (blocking) {
            blockOnFutures(futures);
        }
    }

    public static  <C extends TcpChannel<C>> void closeServerChannels(String profile, List<C> channels, Logger logger) {
        ArrayList<ListenableActionFuture<C>> futures = new ArrayList<>(channels.size());
        for (final C channel : channels) {
            if (channel != null && channel.isOpen()) {
                ListenableActionFuture<C> f = channel.closeAsync();
                f.addListener(ActionListener.wrap(c -> {},
                    e -> logger.warn(() -> new ParameterizedMessage("Error closing serverChannel for profile [{}]", profile), e)));
                futures.add(f);
            }
        }

        blockOnFutures(futures);
    }

    private static <C extends TcpChannel<C>> void blockOnFutures(ArrayList<ListenableActionFuture<C>> futures) {
        for (ListenableActionFuture<C> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                // Ignore as we already attached a listener to log
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Future got interrupted", e);
            }
        }
    }
}
