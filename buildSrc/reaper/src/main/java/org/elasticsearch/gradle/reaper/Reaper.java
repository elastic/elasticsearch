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

package org.elasticsearch.gradle.reaper;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A standalone process that will reap external services after a build dies.
 *
 * <h2>Input</h2>
 * Since how to reap a given service is platform and service dependent, this tool
 * operates on system commands to execute. It takes a single argument, a directory
 * that will contain files with reaping commands. Each line in each file will be
 * executed with {@link Runtime#getRuntime()#exec}.
 *
 * The main method will wait indefinitely on the parent process (Gradle) by
 * reading from stdin. When Gradle shuts down, whether normally or abruptly, the
 * pipe will be broken and read will return.
 *
 * The reaper will then iterate over the files in the configured directory,
 * and execute the given commands. If any commands fail, a failure message is
 * written to stderr. Otherwise, the input file will be deleted. If no inputs
 * produced errors, the entire input directory is deleted upon completion of reaping.
 */
public class Reaper implements Closeable {

    Path inputDir;
    boolean failed;

    private Reaper(Path inputDir) {
        this.inputDir = inputDir;
        this.failed = false;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Expected one argument.\nUsage: java -cp reaper.jar Reaper <DIR_OF_REAPING_COMMANDS>");
            System.exit(1);
        }
        Path inputDir = Paths.get(args[0]);

        try (Reaper reaper = new Reaper(inputDir)){
            System.in.read();
            reaper.reap();
        }
    }

    private void reap() {
        try {
            final List<Path> inputFiles = Files.list(inputDir)
                .filter(p -> p.getFileName().toString().endsWith(".log") == false).collect(Collectors.toList());

            for (Path inputFile : inputFiles) {
                System.out.println("Process file: " + inputFile);
                for (String line : Files.readAllLines(inputFile)) {
                    System.out.println("Running command: " + line);
                    String[] command = line.split(" ");
                    Process process = Runtime.getRuntime().exec(command);
                    int ret = process.waitFor();

                    System.out.println("Stdout: " + readStream(process.getInputStream()));
                    System.out.println("Stderr: " + readStream(process.getErrorStream()));
                    if (ret != 0) {
                        logFailure("Command [" + line + "] failed with exit code " + ret, null);
                    }
                }
            }
        } catch (Exception e) {
            logFailure("Failed to reap inputs", e);
        }
    }

    private void logFailure(String message, Exception e) {
        System.err.println(message);
        e.printStackTrace(System.err);
        failed = true;
    }

    private static String readStream(InputStream s) throws IOException {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        s.transferTo(o);
        return o.toString(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        if (failed == false) {
            try {
                for (Path toDelete : Files.walk(inputDir).sorted(Comparator.reverseOrder()).collect(Collectors.toList())) {
                    Files.delete(toDelete);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
