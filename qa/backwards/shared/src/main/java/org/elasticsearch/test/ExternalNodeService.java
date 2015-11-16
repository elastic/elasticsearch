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

package org.elasticsearch.test;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.NamedThreadFactory;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.logging.support.LoggerMessageFormat;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.DelimiterBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.Delimiters;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.util.CharsetUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 * Service that forks and manages Elasticsearch instances. This should be
 * started before any backwards compatibility tests. Its a separate service to
 * keep all forking out of the tests so they can continue to run under seccomp.
 *
 * The service takes single lines of the format "$command $arg $arg $arg\n". It
 * replies over to the command with any number of lines and signals that it is
 * done with the request by terminating the connection. Its up to the client to
 * interpret the reply lines as informational or status.
 */
public class ExternalNodeService {
    public static final int DEFAULT_PORT = 9871;
    public static final String SHUTDOWN_MESSAGE = "shutting down";

    static {
        // Stick this as early as possible to make sure the any Elasticsearch logs get the right prefix.
        System.setProperty("es.logger.prefix", "");
    }

    private static final ESLogger logger = ESLoggerFactory.getLogger("external-node-service");

    public static void main(String[] args) throws IOException, InterruptedException {
        int arg = 0;
        int port = Integer.parseInt(System.getProperty("ens.port", Integer.toString(DEFAULT_PORT)));
        if (args[arg].equals("shutdown")) {
            sendShutdown(port);
            return;
        }
        if (args[arg].equals("kill")) {
            killAllBackwardsNodes();
            return;
        }
        Path elasticsearchStable = elasticsearchStable(args[arg++]);
        boolean block = true;
        if (arg < args.length) {
            block = !"noblock".equals(args[arg++]);
        }

        killAllBackwardsNodes();

        Map<String, Path> elasticsearches = new HashMap<>();
        logger.info("Scanning for elasticsearches...");
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(elasticsearchStable)) {
            for (Path subdir : directoryStream) {
                if (!Files.isDirectory(subdir)) {
                    continue;
                }
                String dirname = subdir.getFileName().toString();
                if (!dirname.startsWith("elasticsearch-")) {
                    continue;
                }
                String version = dirname.substring("elasticsearch-".length());
                logger.info("Found " + version);
                elasticsearches.put(version, subdir);
            }
        }
        if (elasticsearches.isEmpty()) {
            throw new IllegalArgumentException("Couldn't find any elasticsearch installations in " + elasticsearchStable.toAbsolutePath());
        }
        ExternalNodeService service = new ExternalNodeService(port, unmodifiableMap(elasticsearches));
        service.start();
        if (block) {
            /*
             * If run from the command line we want to block. If run from maven
             * we don't want to block. Instead we just let the main thread
             * terminate here so that maven continues with the next steps and
             * the daemon threads handle the connections.
             */
            try {
                while (true) {
                    Thread.sleep(1000);
                }
            } catch (InterruptedException e) {
                // This is just ctrl-c. Its cool.
            }
        }
    }

    @SuppressForbidden(reason = "we don't have an environment to read from")
    private static Path elasticsearchStable(String location) {
        return PathUtils.get(location);
    }

    private static void sendShutdown(int port) {
        new ExternalNodeServiceClient(port).shutdownService();
    }

    private final ExecutorService readLines = Executors.newCachedThreadPool(new DaemonizedThreadFactory("readlines"));
    /**
     * Running elasticsearch instances indexed by port.
     */
    private final ConcurrentMap<String, ProcessInfo> runningElasticsearches = new ConcurrentHashMap<>();
    /**
     * Port on which the daemon should listen.
     */
    private final int port;
    /**
     * Map from version to root of the untarred distribution.
     */
    private final Map<String, Path> elasticsearchDistributions;
    /**
     * Is this service shutting down?
     */
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);

    private Thread shutdownHook;

    private ServerBootstrap server;

    public ExternalNodeService(int port, Map<String, Path> elasticsearches) throws IOException {
        this.port = port;
        this.elasticsearchDistributions = elasticsearches;
    }

    public void start() {
        shutdownHook = new Thread(new ShutdownHandler());
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(new DaemonizedThreadFactory("ens-boss")),
                Executors.newCachedThreadPool(new DaemonizedThreadFactory("ens-worker")));
        ServerBootstrap server = new ServerBootstrap(factory);
        server.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                SimpleChannelHandler handler = new SimpleChannelHandler() {
                    AtomicBoolean sendingError = new AtomicBoolean(false);

                    @Override
                    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
                        String line = (String) e.getMessage();
                        logger.debug("Client sent [{}]", line);
                        Deque<String> commandLine = new LinkedList<>();
                        Collections.addAll(commandLine, line.split(" "));
                        new Handler(e, commandLine).handle();
                        e.getChannel().close();
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
                        if (e.getChannel().isOpen()) {
                            if (sendingError.compareAndSet(false, true)) {
                                e.getChannel().write(ChannelBuffers.copiedBuffer(
                                        "Error processing request:  " + e.getCause().getMessage() + '\n', StandardCharsets.UTF_8));
                                sendingError.set(false);
                            }
                            e.getChannel().close();
                        }
                        logger.error("Error", e.getCause());
                    }
                };
                return Channels.pipeline(new DelimiterBasedFrameDecoder(80960, Delimiters.lineDelimiter()),
                        new StringDecoder(CharsetUtil.UTF_8), handler);
            }
        });

        server.setOption("child.tcpNoDelay", true);
        server.setOption("child.keepAlive", true);
        logger.debug("Binding localhost:" + port + "...");
        try {
            server.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port));
        } catch (UnknownHostException e) {
            throw new RuntimeException("Couldn't find localhost!", e);
        }
        this.server = server;
        logger.info("Bound localhost:" + port);
    }

    /**
     * Called on startup to make sure that all backwards compatibility nodes
     * that might have been created by previous runs of this are dead, dead,
     * dead.
     */
    private static void killAllBackwardsNodes() throws IOException, InterruptedException {
        // This method is 1000% hacks and non-portable workarounds.
        List<String> commandLine = new ArrayList<>();
        String bwcPathPart = "backwards";
        String esPattern = "bootstrap.Elasticsearch";
        if (Constants.WINDOWS) {
            commandLine.add("wmic");
            commandLine.add("process");
            commandLine.add("where");
            commandLine.add("Name like 'java%%.exe' and CommandLine like '%%" + bwcPathPart + "%%' and CommandLine like '%%"
                    + esPattern + "%%'");
            commandLine.add("get");
            commandLine.add("ProcessId");
        } else {
            commandLine.add("bash");
            commandLine.add("-c");
            commandLine.add("ps aux | grep java | grep -v grep | grep " + bwcPathPart + " | grep " + esPattern
                    + " | awk '{print $2}'");
        }
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        builder.redirectErrorStream(true);
        Process process = null;
        BufferedReader stdout = null;
        List<String> lines = new ArrayList<>();
        List<String> pids = new ArrayList<>();
        try {
            process = builder.start();
            stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            if (Constants.WINDOWS) {
                // In windows the first line is a heading or blank line
                lines.add(stdout.readLine());
            }
            String line;
            while ((line = stdout.readLine()) != null) {
                lines.add(line);
                if (line.isEmpty()) {
                    // Windows outputs a bunch of empty lines
                    continue;
                }
                pids.add(line.trim());
            }
            process.waitFor();
            logger.debug("Process list: [{}]", lines);
            if (process.exitValue() != 0) {
                logger.error("Getting pids of backwards nodes failed with output [{}]", lines);
                throw new RuntimeException("Getting pids of backwards nodes failed with exit code: [" + process.exitValue() + ']');
            }
        } finally {
            if (stdout != null) {
                stdout.close();
            }
            if (process != null) {
                process.destroy();
            }
        }
        if (pids.isEmpty() == false) {
            logger.info("Killing backwards nodes running at [{}]", pids);
        }
        for (String pid : pids) {
            new ProcessInfo(null, pid).stop();
        }
    }

    private class Handler {
        private final MessageEvent e;
        private final Deque<String> commandLine;

        public Handler(MessageEvent e, Deque<String> commandLine) {
            this.e = e;
            this.commandLine = commandLine;
        }

        private void handle() throws IOException {
            String command = commandLine.pop();
            switch (command) {
            case "start":
                start(commandLine);
                return;
            case "stop":
                stop(commandLine);
                return;
            case "shutdown":
                message(SHUTDOWN_MESSAGE);
                // Can't run shutdown directly because netty complains that it could cause a deadlock.
                shutdownHook.start();
                return;
            default:
                message("unknown command: " + command);
                return;
            }
        }

        private void start(Deque<String> commandLine) throws IOException {
            if (commandLine.isEmpty()) {
                message("No version sent!");
                return;
            }
            String version = commandLine.pop();
            Path versionRoot = elasticsearchDistributions.get(version);
            if (versionRoot == null) {
                message("Version not found: " + version);
                return;
            }

            message("starting elasticsearch " + version + "...");

            List<String> command = buildStartCommand(versionRoot.toAbsolutePath().normalize(), commandLine);
            StringBuilder startReproduction = new StringBuilder();
            boolean first = true;
            for (String c : command) {
                if (first) {
                    first = false;
                } else {
                    startReproduction.append(' ');
                }
                startReproduction.append(c);
            }
            logger.debug("Starting elasticsearch with {}", startReproduction);
            ProcessBuilder builder = new ProcessBuilder(command);
            builder.redirectErrorStream(true);
            Process process = null;
            String port = null;
            String pid = null;
            BufferedReader stdout = null;
            try {
                process = builder.start();
                stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
                message("process forked");

                Matcher m = readUntilMatches(stdout, "pid", ".+\\[node.+ pid\\[(\\d+)\\].*", timeValueSeconds(10));
                pid = m.group(1);
                message("pid is [" + pid + "]");
                m = readUntilMatches(stdout, "transport address", ".+\\[transport .+bound_addresses .*\\{(?:127\\.0\\.0\\.1|\\[::1\\]):(\\d+)\\}.*",
                        timeValueSeconds(20));
                port = m.group(1);
                message("bound to [localhost:" + port + "]");
                readUntilMatches(stdout, "started", ".+\\] started$", timeValueSeconds(20));
                runningElasticsearches.put(port, new ProcessInfo(process, pid));
                message("started");
                process.getInputStream().close();
            } finally {
                if (process != null && (port == null || !runningElasticsearches.containsKey(port))) {
                    logger.error("It looks like we failed to launch elasticsearch. Kill -9ing it just to make sure it doesn't linger.");
                    logger.error("We tried to start it like this: {}", startReproduction);
                    // In Java 1.8 this should be destroyForcibly
                    process.destroy();
                    if (pid != null) {
                        try {
                            new ProcessInfo(null, pid).stop();
                        } catch (InterruptedException e) {
                            logger.warn("Failed to stop busted elasticsearch at [{}]", pid);
                        }
                    }
                    if (stdout != null) {
                        try {
                            stdout.reset();
                            CharBuffer lines = CharBuffer.allocate(1024 * 1024);
                            stdout.read(lines);
                            lines.flip();
                            logger.error(
                                    "We were able to capture some of elasticsearch's output. Hopefully this will be useful in figuring out the failure:\n{}",
                                    lines.toString());
                        } catch (IOException e) {
                            logger.warn("Sadly we couldn't capture any of its output.", e);
                        }
                    }
                }
            }
        }

        private void stop(Deque<String> commandLine) {
            if (commandLine.isEmpty()) {
                message("no port sent!");
                return;
            }
            String port = commandLine.pop();
            ProcessInfo elasticsearch = runningElasticsearches.remove(port);
            if (elasticsearch == null) {
                message("couldn't find elasticsearch bound to localhost:" + port + "!");
                return;
            }
            message("killing elasticsearch bound to localhost:" + port);
            try {
                elasticsearch.stop();
            } catch (InterruptedException e) {
                message("timed out waiting for elasticsearch to stop!");
                runningElasticsearches.put(port, elasticsearch);
                return;
            } catch (IOException e) {
                message("error waiting for elasticsearch to stop: [" + e.getMessage() + "]");
                logger.warn("Error waiting for elasticsearch to stop", e);
                runningElasticsearches.put(port, elasticsearch);
                return;
            }
            message("killed");
        }

        private void message(String message) {
            logger.debug("Sending [{}] to client", message);
            e.getChannel().write(ChannelBuffers.copiedBuffer(message + '\n', StandardCharsets.UTF_8));
        }

        private List<String> buildStartCommand(Path versionRoot, Deque<String> commandLine) {
            String executable = Constants.WINDOWS ? "elasticsearch.bat" : "elasticsearch";
            List<String> command = new ArrayList<>();
            command.add(versionRoot.resolve("bin").resolve(executable).toString());
            while (!commandLine.isEmpty()) {
                String arg = commandLine.pop();
                arg = arg.replace("${PATH}", versionRoot.toString());
                command.add(arg);
            }
            // We need at least INFO in http and node
            command.add("-Des.logger.transport=INFO");
            command.add("-Des.logger.node=INFO");
            // It'd be nice to conditionally apply these but that can wait.
            return command;
        }

        private Matcher readUntilMatches(final BufferedReader reader, String description, String pattern, TimeValue timeout) {
            final Pattern compiled = Pattern.compile(pattern);
            /*
             * Use a thread pool here because its relatively simple to timeout
             * the io.
             */
            Future<Matcher> f = readLines.submit(new Callable<Matcher>() {
                @Override
                public Matcher call() throws IOException {
                    ESLogger unformattedLogger = ESLoggerFactory.getLogger("test.external");
                    reader.mark(1024 * 1024);
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Matcher m = compiled.matcher(line);
                        if (m.matches()) {
                            unformattedLogger.debug(line);
                            return m;
                        } else {
                            unformattedLogger.trace(line);
                        }
                    }
                    return null;
                }
            });
            try {
                Matcher m = f.get(timeout.micros(), TimeUnit.MICROSECONDS);
                if (m == null) {
                    throw new ReadFailedException(description);
                }
                return m;
            } catch (TimeoutException e) {
                FutureUtils.cancel(f);
                throw new ReadFailedException(description, e);
            } catch (ExecutionException e) {
                throw new ReadFailedException(description, e);
            } catch (InterruptedException e) {
                Thread.interrupted();
                throw new ReadFailedException(description, e);
            }
        }
    }

    /**
     * Thrown when we fail to capture some output from an output stream.
     */
    private class ReadFailedException extends RuntimeException {
        private ReadFailedException(String description) {
            super("elasticsearch stopped before " + description);
        }

        private ReadFailedException(String description, Exception cause) {
            super("failed to capture " + description, cause);
        }
    }

    /**
     * Catches shutdown and shuts down
     */
    private class ShutdownHandler implements Runnable {
        @Override
        public void run() {
            if (!shuttingDown.compareAndSet(false, true)) {
                logger.debug("Shutting down twice. Weird but ok.");
                return;
            }
            logger.info("Shutting down");
            if (shutdownHook != null) {
                // The hook may be null if we never started
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException e) {
                    // Its cool - this is caused by trying to remove the hook
                    // during shutdown
                }
            }
            for (Map.Entry<String, ProcessInfo> elasticsearch : runningElasticsearches.entrySet()) {
                logger.debug("Kill -9ing elasticsearch running at localhost:{}", elasticsearch.getKey());
                // In Java 1.8 this should be destroyForcibly.
                try {
                    elasticsearch.getValue().stop();
                } catch (IOException | InterruptedException e) {
                    logger.error("Error stopping elasticsearch", e);
                }
            }
            if (server != null) {
                logger.debug("Shutting down server");
                server.shutdown();
            }
        }
    }

    /**
     * Daemonized ThreadFactory used so this won't block in maven.
     */
    private static class DaemonizedThreadFactory extends NamedThreadFactory {
        public DaemonizedThreadFactory(String threadNamePrefix) {
            super(threadNamePrefix);
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = super.newThread(r);
            t.setDaemon(true);
            return t;
        }
    }

    private static class ProcessInfo {
        private final Process process;
        private final String pid;

        public ProcessInfo(@Nullable Process process, String pid) {
            this.process = process;
            this.pid = pid;
        }

        public void stop() throws IOException, InterruptedException {
            /*
             * process.destroy doesn't work properly on windows and sometimes we
             * want to kill by pid so we just go with the super aggressive
             * option every time.....
             */
            List<String> commandLine = new ArrayList<>();
            if (Constants.WINDOWS) {
                commandLine.add("taskkill");
                commandLine.add("/F"); // Force
                commandLine.add("/PID");
                commandLine.add(pid);
            } else {
                commandLine.add("kill");
                commandLine.add("-9"); // Force
                commandLine.add(pid);
            }
            ProcessBuilder builder = new ProcessBuilder(commandLine);
            Process killProcess = null;
            try {
                logger.debug("Killing [{}]", pid);
                killProcess = builder.start();
                killProcess.waitFor();
                if (killProcess.exitValue() != 0) {
                    throw new RuntimeException(
                            LoggerMessageFormat.format("Killing [{}] failed with exit code [{}]", pid, killProcess.exitValue()));
                }
            } finally {
                if (killProcess != null) {
                    killProcess.destroy();
                }
            }
            if (process != null) {
                process.waitFor();
            }
        }
    }
}
