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

package example;

import java.lang.management.ManagementFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

/** Crappy example test fixture that responds with TEST and closes the connection */
public class ExampleTestFixture {
    public static void main(String args[]) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("ExampleTestFixture <logDirectory>");
        }
        Path dir = Paths.get(args[0]);
        AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel
                .open()
                .bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

        // write pid file
        Path tmp = Files.createTempFile(dir, null, null);
        String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        Files.write(tmp, Collections.singleton(pid));
        Files.move(tmp, dir.resolve("pid"), StandardCopyOption.ATOMIC_MOVE);

        // write port file
        tmp = Files.createTempFile(dir, null, null);
        InetSocketAddress bound = (InetSocketAddress) server.getLocalAddress();
        if (bound.getAddress() instanceof Inet6Address) {
            Files.write(tmp, Collections.singleton("[" + bound.getHostString() + "]:" + bound.getPort()));
        } else {
            Files.write(tmp, Collections.singleton(bound.getHostString() + ":" + bound.getPort()));
        }
        Files.move(tmp, dir.resolve("ports"), StandardCopyOption.ATOMIC_MOVE);

        // go time
        server.accept(null, new CompletionHandler<AsynchronousSocketChannel,Void>() {
            @Override
            public void completed(AsynchronousSocketChannel socket, Void attachment) {
                server.accept(null, this);
                try (AsynchronousSocketChannel ch = socket) {
                    ch.write(ByteBuffer.wrap("TEST\n".getBytes(StandardCharsets.UTF_8))).get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {}
        });

        // wait forever, until you kill me
        Thread.sleep(Long.MAX_VALUE);
    }
}
