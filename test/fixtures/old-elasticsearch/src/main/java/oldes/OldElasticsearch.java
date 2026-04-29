/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Starts an unpacked Elasticsearch distribution with {@code http.port: 0} (and matching transport settings),
 * reads startup logs until it learns which HTTP port was chosen, then writes that port to a {@code ports} file for
 * Gradle {@code AntFixture}-based tests.
 * <p>
 * This launcher was introduced because Elasticsearch versions before 5.0 did not write a dedicated ports file. The
 * same mechanism is still reused for newer distributions (for example reindex-from-remote coverage against 7.x/8.x/9.x),
 * where tests download an archive and need a single portable process that exposes the dynamic HTTP port to Gradle
 * without hard-coding platform defaults.
 */
public class OldElasticsearch {

    private static final Pattern INSTALL_DIR_MAJOR_VERSION = Pattern.compile("^elasticsearch(?:-oss)?-(\\d+)");

    /**
     * Prefer matching HTTP bind lines only (logger {@code org.elasticsearch.http.*}). Transport publishes a similar
     * bound-address shape but under {@code org.elasticsearch.transport.*}, so matching generic {@code bound_addresses}
     * text would capture the transport port and HTTP clients would fail.
     */
    private static final Pattern HTTP_PUBLISH_PORT = Pattern.compile(".*\\[o\\.e\\.h\\.[^]]+\\].*publish_address \\{[^}]*:(\\d+)\\}");

    /** Legacy formats used by older distributions (console layout differs by major version). */
    private static final Pattern[] LEGACY_HTTP_PORT_PATTERNS = new Pattern[] {
        Pattern.compile("(\\[http\\s+\\]|Netty4HttpServerTransport|HttpServer).+bound_address.+127\\.0\\.0\\.1:(\\d+)"),
        Pattern.compile("(\\[http\\s+\\]|Netty4HttpServerTransport|HttpServer).+bound_address.+\\[::1\\]:(\\d+)"), };

    /**
     * Reads the install-directory name (for example {@code elasticsearch-8.12.2}) to decide transport settings.
     * When the name is unexpected, returns a value below 8 so legacy {@code transport.tcp.port} is used.
     */
    private static int majorVersionFromInstallDir(Path esInstallDir) {
        String name = esInstallDir.getFileName().toString();
        Matcher m = INSTALL_DIR_MAJOR_VERSION.matcher(name);
        if (m.find() == false) {
            return 0;
        }
        return Integer.parseInt(m.group(1));
    }

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
        configOptions.add("http.port: 0");
        if (majorVersionFromInstallDir(esDir) >= 8) {
            // transport.tcp.port was removed in 8.0; transport.port replaces it
            configOptions.add("transport.port: 0");
        } else {
            configOptions.add("transport.tcp.port: 0");
        }
        configOptions.add("network.host: 127.0.0.1");
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
        /*
         * Gradle starts this JVM with CLASSPATH pointing at the fixture (which depends on the repo's Lucene).
         * Elasticsearch's launch scripts propagate the environment, so a child ES node would then load two
         * incompatible lucene-core versions. Drop inherited CLASSPATH so only the distribution's lib/ is used.
         */
        Map<String, String> childEnv = subprocess.environment();
        childEnv.remove("CLASSPATH");
        subprocess.redirectErrorStream(true);
        Process process = subprocess.start();
        System.out.println("Running " + command);

        int pid = 0;
        int port = 0;

        Pattern pidPattern = Pattern.compile("pid\\[(\\d+)\\]");
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
                if (port == 0) {
                    m = HTTP_PUBLISH_PORT.matcher(line);
                    if (m.find()) {
                        port = Integer.parseInt(m.group(1));
                        System.out.println("Found port (HTTP publish_address):  " + port);
                        continue;
                    }
                    for (Pattern legacy : LEGACY_HTTP_PORT_PATTERNS) {
                        m = legacy.matcher(line);
                        if (m.find()) {
                            port = Integer.parseInt(m.group(2));
                            System.out.println("Found port (legacy HTTP log):  " + port);
                            break;
                        }
                    }
                }
            }
        }

        if (port == 0) {
            System.err.println("port not found");
            System.exit(1);
        }

        Path tmp = Files.createTempFile(baseDir, null, null);
        Files.writeString(tmp, Integer.toString(port));
        Files.move(tmp, baseDir.resolve("ports"), StandardCopyOption.ATOMIC_MOVE);

        tmp = Files.createTempFile(baseDir, null, null);
        Files.writeString(tmp, Integer.toString(pid));
        Files.move(tmp, baseDir.resolve("pid"), StandardCopyOption.ATOMIC_MOVE);
    }
}
