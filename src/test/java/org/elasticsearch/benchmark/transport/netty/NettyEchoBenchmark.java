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

package org.elasticsearch.benchmark.transport.netty;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

public class NettyEchoBenchmark {

    public static void main(String[] args) {
        final int payloadSize = 100;
        int CYCLE_SIZE = 50000;
        final long NUMBER_OF_ITERATIONS = 500000;

        ChannelBuffer message = ChannelBuffers.buffer(100);
        for (int i = 0; i < message.capacity(); i++) {
            message.writeByte((byte) i);
        }

        // Configure the server.
        ServerBootstrap serverBootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

        // Set up the pipeline factory.
        serverBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(new EchoServerHandler());
            }
        });

        // Bind and start to accept incoming connections.
        serverBootstrap.bind(new InetSocketAddress(9000));

        ClientBootstrap clientBootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));

//        ClientBootstrap clientBootstrap = new ClientBootstrap(
//                new OioClientSocketChannelFactory(Executors.newCachedThreadPool()));

        // Set up the pipeline factory.
        final EchoClientHandler clientHandler = new EchoClientHandler();
        clientBootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                return Channels.pipeline(clientHandler);
            }
        });

        // Start the connection attempt.
        ChannelFuture future = clientBootstrap.connect(new InetSocketAddress("localhost", 9000));
        future.awaitUninterruptibly();
        Channel clientChannel = future.getChannel();

        System.out.println("Warming up...");
        for (long i = 0; i < 10000; i++) {
            clientHandler.latch = new CountDownLatch(1);
            clientChannel.write(message);
            try {
                clientHandler.latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Warmed up");


        long start = System.currentTimeMillis();
        long cycleStart = System.currentTimeMillis();
        for (long i = 1; i < NUMBER_OF_ITERATIONS; i++) {
            clientHandler.latch = new CountDownLatch(1);
            clientChannel.write(message);
            try {
                clientHandler.latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if ((i % CYCLE_SIZE) == 0) {
                long cycleEnd = System.currentTimeMillis();
                System.out.println("Ran 50000, TPS " + (CYCLE_SIZE / ((double) (cycleEnd - cycleStart) / 1000)));
                cycleStart = cycleEnd;
            }
        }
        long end = System.currentTimeMillis();
        long seconds = (end - start) / 1000;
        System.out.println("Ran [" + NUMBER_OF_ITERATIONS + "] iterations, payload [" + payloadSize + "]: took [" + seconds + "], TPS: " + ((double) NUMBER_OF_ITERATIONS) / seconds);

        clientChannel.close().awaitUninterruptibly();
        clientBootstrap.releaseExternalResources();
        serverBootstrap.releaseExternalResources();
    }

    public static class EchoClientHandler extends SimpleChannelUpstreamHandler {

        public volatile CountDownLatch latch;

        public EchoClientHandler() {
        }

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            latch.countDown();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            e.getCause().printStackTrace();
            e.getChannel().close();
        }
    }


    public static class EchoServerHandler extends SimpleChannelUpstreamHandler {

        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
            e.getChannel().write(e.getMessage());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
            // Close the connection when an exception is raised.
            e.getCause().printStackTrace();
            e.getChannel().close();
        }
    }
}