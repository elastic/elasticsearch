/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package oldes;

import org.apache.lucene.util.Constants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Starts a version of Elasticsearch that has been unzipped into an empty directory,
 * instructing it to ask the OS for an unused port, grepping the logs for the port
 * it actually got, and writing a {@code ports} file with the port. This is only
 * required for versions of Elasticsearch before 5.0 because they do not support
 * writing a "ports" file.
 */
public class OldElasticsearch {
    public static void main(String[] args) throws IOException {
        Path baseDir = Paths.get(args[0]);
        Path unzipDir = Paths.get(args[1]);

        // 0.90 must be explicitly foregrounded
        boolean explicitlyForeground;
        switch (args[2]) {
            case "true" -> explicitlyForeground = true;
            case "false" -> explicitlyForeground = false;
            default -> {
                System.err.println("the third argument must be true or false");
                System.exit(1);
                return;
            }
        }

        Iterator<Path> children = Files.list(unzipDir).iterator();
        if (false == children.hasNext()) {
            System.err.println("expected the es directory to contain a single child directory but contained none.");
            System.exit(1);
        }
        Path esDir = children.next();
        if (children.hasNext()) {
            System.err.println(
                "expected the es directory to contains a single child directory but contained ["
                    + esDir
                    + "] and ["
                    + children.next()
                    + "]."
            );
            System.exit(1);
        }
        if (false == Files.isDirectory(esDir)) {
            System.err.println("expected the es directory to contains a single child directory but contained a single child file.");
            System.exit(1);
        }

        Path bin = esDir.resolve("bin").resolve("elasticsearch" + (Constants.WINDOWS ? ".bat" : ""));
        Path config = esDir.resolve("config").resolve("elasticsearch.yml");

        List<String> configOptions = new ArrayList<>();
        configOptions.addAll(Arrays.asList("http.port: 0", "transport.tcp.port: 0", "network.host: 127.0.0.1"));
        if (args.length > 3) {
            for (int i = 3; i < args.length; i++) {
                configOptions.add(args[i]);
            }
        }

        Files.write(config, configOptions, StandardCharsets.UTF_8);

        List<String> command = new ArrayList<>();
        command.add(bin.toString());
        if (explicitlyForeground) {
            command.add("-f");
        }
        command.add("-p");
        Path pidPath = baseDir.relativize(baseDir.resolve("pid"));
        command.add(pidPath.toString().replace("&", "\\&"));
        ProcessBuilder subprocess = new ProcessBuilder(command);
        Process process = subprocess.start();
        System.out.println("Running " + command);

        int pid = 0;
        int port = 0;

        Pattern pidPattern = Pattern.compile("pid\\[(\\d+)\\]");
        Pattern httpPortPattern = Pattern.compile(
            "(\\[http\\s+\\]|Netty4HttpServerTransport|HttpServer).+bound_address.+127\\.0\\.0\\.1:(\\d+)"
        );
        try (BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = stdout.readLine()) != null && (pid == 0 || port == 0)) {
                System.out.println(line);
                Matcher m = pidPattern.matcher(line);
                if (m.find()) {
                    pid = Integer.parseInt(m.group(1));
                    System.out.println("Found pid:  " + pid);
                    continue;
                }
                m = httpPortPattern.matcher(line);
                if (m.find()) {
                    port = Integer.parseInt(m.group(2));
                    System.out.println("Found port:  " + port);
                    continue;
                }
            }
        }

        if (port == 0) {
            System.err.println("port not found");
            System.exit(1);
        }

        Path tmp = Files.createTempFile(baseDir, null, null);
        Files.write(tmp, Integer.toString(port).getBytes(StandardCharsets.UTF_8));
        Files.move(tmp, baseDir.resolve("ports"), StandardCopyOption.ATOMIC_MOVE);

        tmp = Files.createTempFile(baseDir, null, null);
        Files.write(tmp, Integer.toString(pid).getBytes(StandardCharsets.UTF_8));
        Files.move(tmp, baseDir.resolve("pid"), StandardCopyOption.ATOMIC_MOVE);
    }
}
