/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.readiness;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

public class ReadinessProbeCli extends EnvironmentAwareCommand {

    private final OptionSpec<Integer> portOption;

    public ReadinessProbeCli() {
        super("A CLI tool to check if Elasticsearch is ready. Exits with return code of 0 if ready.", () -> {});
        portOption = parser.acceptsAll(Arrays.asList("p", "port"), "Target port").withRequiredArg().ofType(Integer.class).defaultsTo(9400);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        tryConnect(options.valueOf(portOption));
    }

    @SuppressForbidden(reason = "Intentional socket open")
    void tryConnect(Integer port) throws IOException {
        InetSocketAddress socketAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
        try (SocketChannel channel = SocketChannel.open(socketAddress)) {
            channel.connect(socketAddress);
            Optional<String> message = readSocketMessage(channel);
            if (message.isEmpty()) {
                throw new NotReadyException();
            }
            if (message.get().startsWith("true") == false) {
                throw new NotReadyException();
            }
        }
    }

    static Optional<String> readSocketMessage(SocketChannel channel) throws IOException {
        BufferedReader reader = new BufferedReader(Channels.newReader(channel, StandardCharsets.UTF_8));
        return Optional.of(reader.readLine());
    }

    private static class NotReadyException extends RuntimeException {}

    public static void main(String[] args) throws Exception {
        try {
            exit(new ReadinessProbeCli().main(args, Terminal.DEFAULT));
        } catch (NotReadyException | IOException expected) {
            exit(1);
        }
    }
}
