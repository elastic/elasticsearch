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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.nio.channel.NioChannel;
import org.elasticsearch.transport.nio.channel.NioServerSocketChannel;
import org.elasticsearch.transport.nio.channel.NioSocketChannel;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

public class OpenChannels implements Releasable {

    // TODO: Maybe set concurrency levels?
    private final ConcurrentMap<NioSocketChannel, Long> openClientChannels = newConcurrentMap();
    private final ConcurrentMap<NioSocketChannel, Long> openAcceptedChannels = newConcurrentMap();
    private final ConcurrentMap<NioServerSocketChannel, Long> openServerChannels = newConcurrentMap();

    private final Logger logger;

    public OpenChannels(Logger logger) {
        this.logger = logger;
    }

    public void serverChannelOpened(NioServerSocketChannel channel) {
        boolean added = openServerChannels.putIfAbsent(channel, System.nanoTime()) == null;
        if (added && logger.isTraceEnabled()) {
            logger.trace("server channel opened: {}", channel);
        }
    }

    public long serverChannelsCount() {
        return openServerChannels.size();
    }

    public void acceptedChannelOpened(NioSocketChannel channel) {
        boolean added = openAcceptedChannels.putIfAbsent(channel, System.nanoTime()) == null;
        if (added && logger.isTraceEnabled()) {
            logger.trace("accepted channel opened: {}", channel);
        }
    }

    public HashSet<NioSocketChannel> getAcceptedChannels() {
        return new HashSet<>(openAcceptedChannels.keySet());
    }

    public void clientChannelOpened(NioSocketChannel channel) {
        boolean added = openClientChannels.putIfAbsent(channel, System.nanoTime()) == null;
        if (added && logger.isTraceEnabled()) {
            logger.trace("client channel opened: {}", channel);
        }
    }

    public Map<NioSocketChannel, Long> getClientChannels() {
        return openClientChannels;
    }

    public void channelClosed(NioChannel channel) {
        boolean removed;
        if (channel instanceof NioServerSocketChannel) {
            removed = openServerChannels.remove(channel) != null;
        } else {
            NioSocketChannel socketChannel = (NioSocketChannel) channel;
            removed = openClientChannels.remove(socketChannel) != null;
            if (removed == false) {
                removed = openAcceptedChannels.remove(socketChannel) != null;
            }
        }
        if (removed && logger.isTraceEnabled()) {
            logger.trace("channel closed: {}", channel);
        }
    }

    public void closeServerChannels() {
        TcpChannel.closeChannels(new ArrayList<>(openServerChannels.keySet()), true);

        openServerChannels.clear();
    }

    @Override
    public void close() {
        Stream<NioChannel> channels = Stream.concat(openClientChannels.keySet().stream(), openAcceptedChannels.keySet().stream());
        TcpChannel.closeChannels(channels.collect(Collectors.toList()), true);

        openClientChannels.clear();
        openAcceptedChannels.clear();
    }
}
