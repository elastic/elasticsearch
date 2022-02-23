/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.readiness;

import joptsimple.OptionSet;

import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.env.Environment;
import org.elasticsearch.readiness.ReadinessService;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Optional;

public class ReadinessProbeCli extends EnvironmentAwareCommand {

    public ReadinessProbeCli() {
        super("A CLI tool to check if Elasticsearch is ready. Exits with return code of 0 if ready.", () -> {});
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        tryConnect(getSocketPath(env));
    }

    Path getSocketPath(Environment environment) {
        return environment.logsFile().resolve(ReadinessService.SOCKET_NAME);
    }

    @SuppressForbidden(reason = "Intentional socket open")
    void tryConnect(Path socketPath) throws IOException {
        UnixDomainSocketAddress socketAddress = UnixDomainSocketAddress.of(socketPath);

        try (SocketChannel channel = SocketChannel.open(StandardProtocolFamily.UNIX)) {
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
