/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.readiness;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.SuppressForbidden;
import org.elasticsearch.cli.Terminal;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;

public class ReadinessCheckCli extends Command {

    private final OptionSpec<String> hostOption;
    private final OptionSpec<Integer> portOption;

    public ReadinessCheckCli() {
        super("A CLI tool to check if Elasticsearch is ready. Exits with return code of 0 if ready.", () -> {});
        hostOption = parser.acceptsAll(Arrays.asList("h", "host"), "Target hostname").withRequiredArg().defaultsTo("localhost");
        portOption = parser.acceptsAll(Arrays.asList("p", "port"), "Target port").withRequiredArg().ofType(Integer.class).defaultsTo(9200);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws IOException {
        Integer port = options.valueOf(portOption);
        String host = options.valueOf(hostOption);

        tryConnect(host, port);
    }

    @SuppressForbidden(reason = "Intentional socket open. InetSocketAddress is used by the constructor.")
    void tryConnect(String host, Integer port) throws IOException {
        try (SocketChannel ignored = SocketChannel.open(new InetSocketAddress(host, port))) {}
    }

    public static void main(String[] args) throws Exception {
        try {
            exit(new ReadinessCheckCli().main(args, Terminal.DEFAULT));
        } catch (IOException expected) {
            exit(1);
        }
    }
}
