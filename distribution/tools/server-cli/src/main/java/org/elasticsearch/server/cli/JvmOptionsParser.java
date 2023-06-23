/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.bootstrap.ServerArgs;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Parses JVM options from a file and prints a single line with all JVM options to standard output.
 */
final class JvmOptionsParser {

    static class JvmOptionsFileParserException extends Exception {

        private final Path jvmOptionsFile;

        Path jvmOptionsFile() {
            return jvmOptionsFile;
        }

        private final SortedMap<Integer, String> invalidLines;

        SortedMap<Integer, String> invalidLines() {
            return invalidLines;
        }

        JvmOptionsFileParserException(final Path jvmOptionsFile, final SortedMap<Integer, String> invalidLines) {
            this.jvmOptionsFile = jvmOptionsFile;
            this.invalidLines = invalidLines;
        }

    }

    /**
     * Determines the jvm options that should be passed to the Elasticsearch Java process.
     *
     * <p> This method works by joining the options found in {@link SystemJvmOptions}, the {@code jvm.options} file,
     * files in the {@code jvm.options.d} directory, and the options given by the {@code ES_JAVA_OPTS} environment
     * variable.
     *
     * @param args            the start-up arguments
     * @param configDir       the ES config dir
     * @param tmpDir          the directory that should be passed to {@code -Djava.io.tmpdir}
     * @param envOptions      the options passed through the ES_JAVA_OPTS env var
     * @return the list of options to put on the Java command line
     * @throws InterruptedException if the java subprocess is interrupted
     * @throws IOException          if there is a problem reading any of the files
     * @throws UserException        if there is a problem parsing the `jvm.options` file or `jvm.options.d` files
     */
    static List<String> determineJvmOptions(ServerArgs args, Path configDir, Path tmpDir, String envOptions) throws InterruptedException,
        IOException, UserException {

        final JvmOptionsParser parser = new JvmOptionsParser();

        final Map<String, String> substitutions = new HashMap<>();
        substitutions.put("ES_TMPDIR", tmpDir.toString());
        substitutions.put("ES_PATH_CONF", configDir.toString());

        try {
            return parser.jvmOptions(args, configDir, tmpDir, envOptions, substitutions);
        } catch (final JvmOptionsFileParserException e) {
            final String errorMessage = String.format(
                Locale.ROOT,
                "encountered [%d] error%s parsing [%s]%s",
                e.invalidLines().size(),
                e.invalidLines().size() == 1 ? "" : "s",
                e.jvmOptionsFile(),
                System.lineSeparator()
            );
            StringBuilder msg = new StringBuilder(errorMessage);
            int count = 0;
            for (final Map.Entry<Integer, String> entry : e.invalidLines().entrySet()) {
                count++;
                final String message = String.format(
                    Locale.ROOT,
                    "[%d]: encountered improperly formatted JVM option in [%s] on line number [%d]: [%s]%s",
                    count,
                    e.jvmOptionsFile(),
                    entry.getKey(),
                    entry.getValue(),
                    System.lineSeparator()
                );
                msg.append(message);
            }
            throw new UserException(ExitCodes.CONFIG, msg.toString());
        }
    }

    private List<String> jvmOptions(
        ServerArgs args,
        final Path config,
        Path tmpDir,
        final String esJavaOpts,
        final Map<String, String> substitutions
    ) throws InterruptedException, IOException, JvmOptionsFileParserException, UserException {

        final List<String> jvmOptions = readJvmOptionsFiles(config);

        if (esJavaOpts != null) {
            jvmOptions.addAll(Arrays.stream(esJavaOpts.split("\\s+")).filter(Predicate.not(String::isBlank)).toList());
        }

        final List<String> substitutedJvmOptions = substitutePlaceholders(jvmOptions, Collections.unmodifiableMap(substitutions));
        final MachineDependentHeap machineDependentHeap = new MachineDependentHeap(
            new OverridableSystemMemoryInfo(substitutedJvmOptions, new DefaultSystemMemoryInfo())
        );
        substitutedJvmOptions.addAll(machineDependentHeap.determineHeapSettings(config, substitutedJvmOptions));
        final List<String> ergonomicJvmOptions = JvmErgonomics.choose(substitutedJvmOptions);
        final List<String> systemJvmOptions = SystemJvmOptions.systemJvmOptions();

        final List<String> apmOptions = APMJvmOptions.apmJvmOptions(args.nodeSettings(), args.secrets(), tmpDir);

        final List<String> finalJvmOptions = new ArrayList<>(
            systemJvmOptions.size() + substitutedJvmOptions.size() + ergonomicJvmOptions.size() + apmOptions.size()
        );
        finalJvmOptions.addAll(systemJvmOptions); // add the system JVM options first so that they can be overridden
        finalJvmOptions.addAll(substitutedJvmOptions);
        finalJvmOptions.addAll(ergonomicJvmOptions);
        finalJvmOptions.addAll(apmOptions);

        return finalJvmOptions;
    }

    List<String> readJvmOptionsFiles(final Path config) throws IOException, JvmOptionsFileParserException {
        final ArrayList<Path> jvmOptionsFiles = new ArrayList<>();
        jvmOptionsFiles.add(config.resolve("jvm.options"));

        final Path jvmOptionsDirectory = config.resolve("jvm.options.d");

        if (Files.isDirectory(jvmOptionsDirectory)) {
            try (DirectoryStream<Path> jvmOptionsDirectoryStream = Files.newDirectoryStream(config.resolve("jvm.options.d"), "*.options")) {
                // collect the matching JVM options files after sorting them by Path::compareTo
                StreamSupport.stream(jvmOptionsDirectoryStream.spliterator(), false).sorted().forEach(jvmOptionsFiles::add);
            }
        }

        final List<String> jvmOptions = new ArrayList<>();

        for (final Path jvmOptionsFile : jvmOptionsFiles) {
            final SortedMap<Integer, String> invalidLines = new TreeMap<>();
            try (
                InputStream is = Files.newInputStream(jvmOptionsFile);
                Reader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(reader)
            ) {
                parse(Runtime.version().feature(), br, jvmOptions::add, invalidLines::put);
            }
            if (invalidLines.isEmpty() == false) {
                throw new JvmOptionsFileParserException(jvmOptionsFile, invalidLines);
            }
        }
        return jvmOptions;
    }

    static List<String> substitutePlaceholders(final List<String> jvmOptions, final Map<String, String> substitutions) {
        final Map<String, String> placeholderSubstitutions = substitutions.entrySet()
            .stream()
            .collect(Collectors.toMap(e -> "${" + e.getKey() + "}", Map.Entry::getValue));
        return jvmOptions.stream().map(jvmOption -> {
            String actualJvmOption = jvmOption;
            int start = jvmOption.indexOf("${");
            if (start >= 0 && jvmOption.indexOf('}', start) > 0) {
                for (final Map.Entry<String, String> placeholderSubstitution : placeholderSubstitutions.entrySet()) {
                    actualJvmOption = actualJvmOption.replace(placeholderSubstitution.getKey(), placeholderSubstitution.getValue());
                }
            }
            return actualJvmOption;
        }).collect(Collectors.toList());
    }

    /**
     * Callback for valid JVM options.
     */
    interface JvmOptionConsumer {
        /**
         * Invoked when a line in the JVM options file matches the specified syntax and the specified major version.
         * @param jvmOption the matching JVM option
         */
        void accept(String jvmOption);
    }

    /**
     * Callback for invalid lines in the JVM options.
     */
    interface InvalidLineConsumer {
        /**
         * Invoked when a line in the JVM options does not match the specified syntax.
         */
        void accept(int lineNumber, String line);
    }

    private static final Pattern PATTERN = Pattern.compile("((?<start>\\d+)(?<range>-)?(?<end>\\d+)?:)?(?<option>-.*)$");

    /**
     * Parse the line-delimited JVM options from the specified buffered reader for the specified Java major version.
     * Valid JVM options are:
     * <ul>
     *     <li>
     *         a line starting with a dash is treated as a JVM option that applies to all versions
     *     </li>
     *     <li>
     *         a line starting with a number followed by a colon is treated as a JVM option that applies to the matching Java major version
     *         only
     *     </li>
     *     <li>
     *         a line starting with a number followed by a dash followed by a colon is treated as a JVM option that applies to the matching
     *         Java specified major version and all larger Java major versions
     *     </li>
     *     <li>
     *         a line starting with a number followed by a dash followed by a number followed by a colon is treated as a JVM option that
     *         applies to the specified range of matching Java major versions
     *     </li>
     * </ul>
     *
     * For example, if the specified Java major version is 17, the following JVM options will be accepted:
     * <ul>
     *     <li>
     *         {@code -XX:+PrintGCDateStamps}
     *     </li>
     *     <li>
     *         {@code 17:-XX:+PrintGCDateStamps}
     *     </li>
     *     <li>
     *         {@code 17-:-XX:+PrintGCDateStamps}
     *     </li>
     *     <li>
     *         {@code 17-18:-XX:+PrintGCDateStamps}
     *     </li>
     * </ul>
     * and the following JVM options will not be accepted:
     * <ul>
     *     <li>
     *         {@code 18:-Xlog:age*=trace,gc*,safepoint:file=logs/gc.log:utctime,pid,tags:filecount=32,filesize=64m}
     *     </li>
     *     <li>
     *         {@code 18-:-Xlog:age*=trace,gc*,safepoint:file=logs/gc.log:utctime,pid,tags:filecount=32,filesize=64m}
     *     </li>
     *     <li>
     *         {@code 18-19:-Xlog:age*=trace,gc*,safepoint:file=logs/gc.log:utctime,pid,tags:filecount=32,filesize=64m}
     *     </li>
     * </ul>
     *
     * If the version syntax specified on a line matches the specified JVM options, the JVM option callback will be invoked with the JVM
     * option. If the line does not match the specified syntax for the JVM options, the invalid line callback will be invoked with the
     * contents of the entire line.
     *
     * @param javaMajorVersion the Java major version to match JVM options against
     * @param br the buffered reader to read line-delimited JVM options from
     * @param jvmOptionConsumer the callback that accepts matching JVM options
     * @param invalidLineConsumer a callback that accepts invalid JVM options
     * @throws IOException if an I/O exception occurs reading from the buffered reader
     */
    static void parse(
        final int javaMajorVersion,
        final BufferedReader br,
        final JvmOptionConsumer jvmOptionConsumer,
        final InvalidLineConsumer invalidLineConsumer
    ) throws IOException {
        int lineNumber = 0;
        while (true) {
            final String line = br.readLine();
            lineNumber++;
            if (line == null) {
                break;
            }
            if (line.startsWith("#")) {
                // lines beginning with "#" are treated as comments
                continue;
            }
            if (line.matches("\\s*")) {
                // skip blank lines
                continue;
            }
            final Matcher matcher = PATTERN.matcher(line);
            if (matcher.matches()) {
                final String start = matcher.group("start");
                final String end = matcher.group("end");
                if (start == null) {
                    // no range present, unconditionally apply the JVM option
                    jvmOptionConsumer.accept(line);
                } else {
                    final int lower;
                    try {
                        lower = Integer.parseInt(start);
                    } catch (final NumberFormatException e) {
                        invalidLineConsumer.accept(lineNumber, line);
                        continue;
                    }
                    final int upper;
                    if (matcher.group("range") == null) {
                        // no range is present, apply the JVM option to the specified major version only
                        upper = lower;
                    } else if (end == null) {
                        // a range of the form \\d+- is present, apply the JVM option to all major versions larger than the specified one
                        upper = Integer.MAX_VALUE;
                    } else {
                        // a range of the form \\d+-\\d+ is present, apply the JVM option to the specified range of major versions
                        try {
                            upper = Integer.parseInt(end);
                        } catch (final NumberFormatException e) {
                            invalidLineConsumer.accept(lineNumber, line);
                            continue;
                        }
                        if (upper < lower) {
                            invalidLineConsumer.accept(lineNumber, line);
                            continue;
                        }
                    }
                    if (lower <= javaMajorVersion && javaMajorVersion <= upper) {
                        jvmOptionConsumer.accept(matcher.group("option"));
                    }
                }
            } else {
                invalidLineConsumer.accept(lineNumber, line);
            }
        }
    }

}
