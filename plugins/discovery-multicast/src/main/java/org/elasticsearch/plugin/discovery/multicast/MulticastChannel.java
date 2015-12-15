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

package org.elasticsearch.plugin.discovery.multicast;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;

import java.io.Closeable;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 * A multicast channel that supports registering for receive events, and sending datagram packets. Allows
 * to easily share the same multicast socket if it holds the same config.
 */
abstract class MulticastChannel implements Closeable {

    /**
     * Builds a channel based on the provided config, allowing to control if sharing a channel that uses
     * the same config is allowed or not.
     */
    public static MulticastChannel getChannel(String name, boolean shared, Config config, Listener listener) throws Exception {
        if (!shared) {
            return new Plain(listener, name, config);
        }
        return Shared.getSharedChannel(listener, config);
    }

    /**
     * Config of multicast channel.
     */
    public static final class Config {
        public final int port;
        public final String group;
        public final int bufferSize;
        public final int ttl;
        public final InetAddress multicastInterface;
        public final boolean deferToInterface;

        public Config(int port, String group, int bufferSize, int ttl,
                      InetAddress multicastInterface, boolean deferToInterface) {
            this.port = port;
            this.group = group;
            this.bufferSize = bufferSize;
            this.ttl = ttl;
            this.multicastInterface = multicastInterface;
            this.deferToInterface = deferToInterface;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Config config = (Config) o;

            if (bufferSize != config.bufferSize) return false;
            if (port != config.port) return false;
            if (ttl != config.ttl) return false;
            if (group != null ? !group.equals(config.group) : config.group != null) return false;
            if (multicastInterface != null ? !multicastInterface.equals(config.multicastInterface) : config.multicastInterface != null)
                return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = port;
            result = 31 * result + (group != null ? group.hashCode() : 0);
            result = 31 * result + bufferSize;
            result = 31 * result + ttl;
            result = 31 * result + (multicastInterface != null ? multicastInterface.hashCode() : 0);
            return result;
        }
    }

    /**
     * Listener that gets called when data is received on the multicast channel.
     */
    public static interface Listener {
        void onMessage(BytesReference data, SocketAddress address);
    }

    /**
     * Simple listener that wraps multiple listeners into one.
     */
    public static class MultiListener implements Listener {

        private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

        public void add(Listener listener) {
            this.listeners.add(listener);
        }

        public boolean remove(Listener listener) {
            return this.listeners.remove(listener);
        }

        @Override
        public void onMessage(BytesReference data, SocketAddress address) {
            for (Listener listener : listeners) {
                listener.onMessage(data, address);
            }
        }
    }

    protected final Listener listener;
    private AtomicBoolean closed = new AtomicBoolean();

    protected MulticastChannel(Listener listener) {
        this.listener = listener;
    }

    /**
     * Send the data over the multicast channel.
     */
    public abstract void send(BytesReference data) throws Exception;

    /**
     * Close the channel.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            close(listener);
        }
    }

    protected abstract void close(Listener listener);

    public static final String SHARED_CHANNEL_NAME = "#shared#";
    /**
     * A shared channel that keeps a static map of Config -&gt; Shared channels, and closes shared
     * channel once their reference count has reached 0. It also handles de-registering relevant
     * listener from the shared list of listeners.
     */
    private final static class Shared extends MulticastChannel {

        private static final Map<Config, Shared> sharedChannels = new HashMap<>();
        private static final Object mutex = new Object(); // global mutex so we don't sync on static methods (.class)

        static MulticastChannel getSharedChannel(Listener listener, Config config) throws Exception {

            synchronized (mutex) {
                Shared shared = sharedChannels.get(config);
                if (shared != null) {
                    shared.incRef();
                    ((MultiListener) shared.listener).add(listener);
                } else {
                    MultiListener multiListener = new MultiListener();
                    multiListener.add(listener);
                    shared = new Shared(multiListener, new Plain(multiListener, SHARED_CHANNEL_NAME, config));
                    sharedChannels.put(config, shared);
                }
                return new Delegate(listener, shared);
            }
        }

        static void close(Shared shared, Listener listener) {
            synchronized (mutex) {
                // remove this
                boolean removed = ((MultiListener) shared.listener).remove(listener);
                assert removed : "a listener should be removed";
                if (shared.decRef() == 0) {
                    assert ((MultiListener) shared.listener).listeners.isEmpty();
                    sharedChannels.remove(shared.channel.getConfig());
                    shared.channel.close();
                }
            }
        }

        final Plain channel;
        private int refCount = 1;

        Shared(MultiListener listener, Plain channel) {
            super(listener);
            this.channel = channel;
        }

        private void incRef() {
            refCount++;
        }

        private int decRef() {
            --refCount;
            assert refCount >= 0 : "illegal ref counting, close called multiple times";
            return refCount;
        }

        @Override
        public void send(BytesReference data) throws Exception {
            channel.send(data);
        }

        @Override
        public void close() {
            assert false : "Shared references should never be closed directly, only via Delegate";
        }

        @Override
        protected void close(Listener listener) {
            close(this, listener);
        }
    }

    /**
     * A light weight delegate that wraps another channel, mainly to support delegating
     * the close method with the provided listener and not holding existing listener.
     */
    private final static class Delegate extends MulticastChannel {

        private final MulticastChannel channel;

        Delegate(Listener listener, MulticastChannel channel) {
            super(listener);
            this.channel = channel;
        }

        @Override
        public void send(BytesReference data) throws Exception {
            channel.send(data);
        }

        @Override
        protected void close(Listener listener) {
            channel.close(listener); // we delegate here to the close with our listener, not with the delegate listener
        }
    }

    /**
     * Simple implementation of a channel.
     */
    @SuppressForbidden(reason = "I bind to wildcard addresses. I am a total nightmare")
    private static class Plain extends MulticastChannel {
        private final ESLogger logger;
        private final Config config;

        private volatile MulticastSocket multicastSocket;
        private final DatagramPacket datagramPacketSend;
        private final DatagramPacket datagramPacketReceive;

        private final Object sendMutex = new Object();
        private final Object receiveMutex = new Object();

        private final Receiver receiver;
        private final Thread receiverThread;

        Plain(Listener listener, String name, Config config) throws Exception {
            super(listener);
            this.logger = ESLoggerFactory.getLogger(name);
            this.config = config;
            this.datagramPacketReceive = new DatagramPacket(new byte[config.bufferSize], config.bufferSize);
            this.datagramPacketSend = new DatagramPacket(new byte[config.bufferSize], config.bufferSize, InetAddress.getByName(config.group), config.port);
            this.multicastSocket = buildMulticastSocket(config);
            this.receiver = new Receiver();
            this.receiverThread = daemonThreadFactory(Settings.builder().put("name", name).build(), "discovery#multicast#receiver").newThread(receiver);
            this.receiverThread.start();
        }

        private MulticastSocket buildMulticastSocket(Config config) throws Exception {
            SocketAddress addr = new InetSocketAddress(InetAddress.getByName(config.group), config.port);
            MulticastSocket multicastSocket = new MulticastSocket(config.port);
            try {
                multicastSocket.setTimeToLive(config.ttl);
                // OSX is not smart enough to tell that a socket bound to the
                // 'lo0' interface needs to make sure to send the UDP packet
                // out of the lo0 interface, so we need to do some special
                // workarounds to fix it.
                if (config.deferToInterface) {
                    // 'null' here tells the socket to deter to the interface set
                    // with .setInterface
                    multicastSocket.joinGroup(addr, null);
                    multicastSocket.setInterface(config.multicastInterface);
                } else {
                    multicastSocket.setInterface(config.multicastInterface);
                    multicastSocket.joinGroup(InetAddress.getByName(config.group));
                }
                multicastSocket.setReceiveBufferSize(config.bufferSize);
                multicastSocket.setSendBufferSize(config.bufferSize);
                multicastSocket.setSoTimeout(60000);
            } catch (Throwable e) {
                IOUtils.closeWhileHandlingException(multicastSocket);
                throw e;
            }
            return multicastSocket;
        }

        public Config getConfig() {
            return this.config;
        }

        @Override
        public void send(BytesReference data) throws Exception {
            synchronized (sendMutex) {
                datagramPacketSend.setData(data.toBytes());
                multicastSocket.send(datagramPacketSend);
            }
        }

        @Override
        protected void close(Listener listener) {
            receiver.stop();
            receiverThread.interrupt();
            if (multicastSocket != null) {
                IOUtils.closeWhileHandlingException(multicastSocket);
                multicastSocket = null;
            }
            try {
                receiverThread.join(10000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private class Receiver implements Runnable {

            private volatile boolean running = true;

            public void stop() {
                running = false;
            }

            @Override
            public void run() {
                while (running) {
                    try {
                        synchronized (receiveMutex) {
                            try {
                                multicastSocket.receive(datagramPacketReceive);
                            } catch (SocketTimeoutException ignore) {
                                continue;
                            } catch (Exception e) {
                                if (running) {
                                    if (multicastSocket.isClosed()) {
                                        logger.warn("multicast socket closed while running, restarting...");
                                        multicastSocket = buildMulticastSocket(config);
                                    } else {
                                        logger.warn("failed to receive packet, throttling...", e);
                                        Thread.sleep(500);
                                    }
                                }
                                continue;
                            }
                        }
                        if (datagramPacketReceive.getData().length > 0) {
                            listener.onMessage(new BytesArray(datagramPacketReceive.getData()), datagramPacketReceive.getSocketAddress());
                        }
                    } catch (Throwable e) {
                        if (running) {
                            logger.warn("unexpected exception in multicast receiver", e);
                        }
                    }
                }
            }
        }
    }
}
