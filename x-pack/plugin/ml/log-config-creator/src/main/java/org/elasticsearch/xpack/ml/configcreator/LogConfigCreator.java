/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import joptsimple.OptionSet;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.PathUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LogConfigCreator extends Command {

    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("(?i)[a-z](?:[a-z0-9_.-]*[a-z0-9])?");

    public static void main(String[] args) throws Exception {
        exit(new LogConfigCreator().main(args, Terminal.DEFAULT));
    }

    LogConfigCreator() {
        super("Log config creator", () -> {});
        parser.acceptsAll(Arrays.asList("o", "output"), "output directory (default .)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("i", "index"), "index for logstash stdin config (default test)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("n", "name"), "name for this type of log file (default xyz)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("b", "beats-repo"), "path to beats repo (default $HOME/beats)").withRequiredArg();
        parser.nonOptions("file to be processed");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {

        Path outputDirectory = parsePath(getSingleOptionValueOrDefault(options, "o", "output", "output directory", "."));
        if (Files.isDirectory(outputDirectory) == false) {
            throw new UserException(ExitCodes.USAGE, "Output directory [" + outputDirectory + "] does not exist");
        }

        String indexName = getSingleOptionValueOrDefault(options, "i", "index", "index name", "test");
        validateName(indexName, "Index name");

        String typeName = getSingleOptionValueOrDefault(options, "n", "name", "log file type name", "xyz");
        validateName(typeName, "Log file type name");

        Path beatsRepo = parsePath(getSingleOptionValueOrDefault(options, "b", "beats-repo", "beats repo directory",
            parsePath(System.getProperty("user.home")).resolve("beats").toAbsolutePath().normalize().toString()));
        if (Files.isDirectory(beatsRepo) == false) {
            if (options.has("b") || options.has("beats-repo")) {
                // Lack of a beats repo is only an error if one was explicitly specified
                throw new UserException(ExitCodes.USAGE, "Beats repo directory [" + outputDirectory + "] does not exist");
            } else {
                beatsRepo = null;
            }
        }

        if (options.nonOptionArguments().size() != 1) {
            throw new UserException(ExitCodes.USAGE, "Exactly one file to analyze must be provided, got [" +
                options.nonOptionArguments().size() + "]");
        }
        Path file = parsePath(options.nonOptionArguments().get(0));
        if (Files.isRegularFile(file) == false) {
            throw new UserException(ExitCodes.USAGE, "[" + file + "] does not exist or is not a file");
        }

        try {
            LogFileStructureFinder structureFinder = new LogFileStructureFinder(terminal, beatsRepo,
                file.toAbsolutePath().normalize().toString(), indexName, typeName);
            structureFinder.findLogFileConfigs(Files.newInputStream(file), outputDirectory);
        } catch (IOException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Cannot determine format of file [" + file + "]: " + e.getMessage());
        }
    }

    private static String getSingleOptionValueOrDefault(OptionSet options, String terseOption, String verboseOption, String description,
                                                        String defaultValue) throws UserException {
        Collection<?> optionValues =
            Stream.concat(options.valuesOf(terseOption).stream(), options.valuesOf(verboseOption).stream()).collect(Collectors.toSet());
        if (optionValues.size() > 1) {
            throw new UserException(ExitCodes.USAGE, "At most one " + description + " may be provided, got [" +
                optionValues.size() + "]");
        }
        return optionValues.isEmpty() ? defaultValue : optionValues.iterator().next().toString();
    }

    private static Path parsePath(Object path) {
        return PathUtils.get(path.toString());
    }

    private static void validateName(String name, String description) throws UserException {
        if (VALID_NAME_PATTERN.matcher(name).matches() == false) {
            throw new UserException(ExitCodes.USAGE, description + " [" + name + "] does not match the acceptable pattern [" +
                VALID_NAME_PATTERN.pattern() + "]");
        }
    }
}
