/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.reaper;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A standalone process that will reap external services after a build dies.
 *
 * <h2>Input</h2>
 * Since how to reap a given service is platform and service dependent, this tool
 * operates on system commands to execute. It takes a single argument, a directory
 * that will contain files with reaping commands. Each line in each file will be
 * executed with {@link Runtime#exec(String)}.
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

    private Path inputDir;
    private boolean failed;

    private Reaper(Path inputDir) {
        this.inputDir = inputDir;
        this.failed = false;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Expected one argument.\nUsage: java -jar reaper.jar <DIR_OF_REAPING_COMMANDS>");
            System.exit(1);
        }
        Path inputDir = Paths.get(args[0]);

        try (Reaper reaper = new Reaper(inputDir)) {
            System.in.read();
            reaper.reap();
        }
    }

    private void reap() {
        try (Stream<Path> stream = Files.list(inputDir)) {
            final List<Path> inputFiles = stream.filter(p -> p.getFileName().toString().endsWith(".cmd")).collect(Collectors.toList());

            for (Path inputFile : inputFiles) {
                System.out.println("Process file: " + inputFile);
                String line = Files.readString(inputFile);
                System.out.println("Running command: " + line);
                String[] command = line.split(" ");
                Process process = Runtime.getRuntime().exec(command);
                int ret = process.waitFor();

                System.out.print("Stdout: ");
                process.getInputStream().transferTo(System.out);
                System.out.print("\nStderr: ");
                process.getErrorStream().transferTo(System.out);
                System.out.println(); // end the stream
                if (ret != 0) {
                    logFailure("Command [" + line + "] failed with exit code " + ret, null);
                } else {
                    delete(inputFile);
                }
            }
        } catch (Exception e) {
            logFailure("Failed to reap inputs", e);
        }
    }

    private void logFailure(String message, Exception e) {
        System.err.println(message);
        if (e != null) {
            e.printStackTrace(System.err);
        }
        failed = true;
    }

    private void delete(Path toDelete) {
        try {
            Files.delete(toDelete);
        } catch (IOException e) {
            logFailure("Failed to delete [" + toDelete + "]", e);
        }
    }

    @Override
    public void close() {
        if (failed == false) {
            try (Stream<Path> stream = Files.walk(inputDir)) {
                stream.sorted(Comparator.reverseOrder()).forEach(this::delete);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
