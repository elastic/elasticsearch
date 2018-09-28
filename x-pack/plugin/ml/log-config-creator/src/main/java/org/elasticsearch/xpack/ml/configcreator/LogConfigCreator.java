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
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.xpack.core.ml.filestructurefinder.FileStructure;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinder;
import org.elasticsearch.xpack.ml.filestructurefinder.FileStructureFinderManager;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Entry point for the log-config-creator program.
 * Determines the most likely location of the Filebeat module directory,
 * handles argument parsing and validation and kicks off the structure finder.
 */
public class LogConfigCreator extends Command {

    private static final Grok IP_OR_HOST_GROK = new Grok(Grok.getBuiltinPatterns(), "^%{IPORHOST}$");

    private static final Path HOME_PATH = parsePath(System.getProperty("user.home"));
    private static final Path REPO_MODULE_PATH =
        HOME_PATH.resolve("beats").resolve("filebeat").resolve("module").toAbsolutePath().normalize();
    private static final Path INSTALL_MODULE_PATH;

    static {
        Path installModulePath = null;

        String progFilesDir = System.getenv("ProgramFiles");
        if (progFilesDir == null || progFilesDir.isEmpty()) {
            Path linuxPackageModulePath = parsePath("/usr/share/filebeat/module");
            if (Files.isDirectory(linuxPackageModulePath)) {
                installModulePath = linuxPackageModulePath;
            }
        } else {
            Path windowsInstallModulePath = parsePath(progFilesDir).resolve("Filebeat").resolve("module");
            if (Files.isDirectory(windowsInstallModulePath)) {
                installModulePath = windowsInstallModulePath;
            }
        }

        // If Filebeat isn't installed in the standard Linux or Windows location, check for
        // a .tar/.zip install that contains modules in the current user's home directory
        if (installModulePath == null) {
            try {
                installModulePath = Files.find(HOME_PATH, 1, (path, attrs) -> path.getFileName().toString().matches("filebeat-.*-.*-.*"))
                    .map(path -> path.resolve("module")).filter(Files::isDirectory).max(Comparator.naturalOrder()).orElse(null);
            } catch (IOException e) {
                // Ignore it - there just won't be a default for the Filebeat installation directory
            }
        }

        INSTALL_MODULE_PATH = installModulePath;
    }

    private static final Pattern VALID_NAME_PATTERN = Pattern.compile("(?i)[a-z](?:[a-z0-9_.-]*[a-z0-9])?");

    public static void main(String[] args) throws Exception {
        exit(new LogConfigCreator().main(args, Terminal.DEFAULT));
    }

    private LogConfigCreator() {
        super("Log config creator", () -> {});
        parser.acceptsAll(Arrays.asList("o", "output"), "output directory (default: .)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("i", "index"), "index for logstash direct from file config (default: test)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("n", "name"), "name for this type of log file (default: xyz)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("f", "filebeat-module-dir"),
            "path to filebeat module directory (default: $HOME/beats/filebeat/module or from installed filebeat)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("e", "elasticsearch-host"), "elasticsearch host (default: localhost)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("l", "logstash-host"), "logstash host (default: localhost)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("c", "sample-line-count"), "how many lines to analyze (default: 1000)").withRequiredArg();
        parser.acceptsAll(Arrays.asList("z", "timezone"),
            "timezone for logstash direct from file input (default: logstash server timezone)").withRequiredArg();
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

        String elasticsearchHost = getSingleOptionValueOrDefault(options, "e", "elasticsearch-host", "elasticsearch host", "localhost");
        validateHost(elasticsearchHost, "Elasticsearch host");

        String logstashHost = getSingleOptionValueOrDefault(options, "l", "logstash-host", "logstash host", "localhost");
        validateHost(logstashHost, "Logstash host");

        String sampleLinesStr = getSingleOptionValueOrDefault(options, "c", "sample-line-count", "value for the number of lines to analyze",
            Integer.toString(FileStructureFinderManager.DEFAULT_IDEAL_SAMPLE_LINE_COUNT));
        int sampleLines;
        try {
            sampleLines = Integer.parseInt(sampleLinesStr);
        } catch (NumberFormatException e) {
            sampleLines = 0;
        }
        if (sampleLines <= FileStructureFinderManager.MIN_SAMPLE_LINE_COUNT) {
            throw new UserException(ExitCodes.USAGE, "Number of lines to analyze must be an integer greater than " +
                FileStructureFinderManager.MIN_SAMPLE_LINE_COUNT + ", got [" + sampleLinesStr + "]");
        }

        Path filebeatModulePath;
        String filebeatModuleDir = getSingleOptionValueOrDefault(options, "f", "filebeat-module-dir", "filebeat module directory", null);
        if (filebeatModuleDir != null) {
            filebeatModulePath = parsePath(filebeatModuleDir).toAbsolutePath().normalize();
            // If a Filebeat module directory is explicitly specified then it must exist
            if (Files.isDirectory(filebeatModulePath) == false) {
                throw new UserException(ExitCodes.USAGE, "Filebeat module directory [" + filebeatModulePath + "] does not exist");
            }
        } else {
            // INSTALL_MODULE_PATH will be null if no install was found
            filebeatModulePath = Files.isDirectory(REPO_MODULE_PATH) ? REPO_MODULE_PATH : INSTALL_MODULE_PATH;
        }

        String timezone = getSingleOptionValueOrDefault(options, "z", "timezone", "timezone name", null);
        try {
            DateTimeZone.forID(timezone);
        } catch (IllegalArgumentException e) {
            throw new UserException(ExitCodes.USAGE, "[" + timezone + "] is not a valid timezone");
        }

        if (options.nonOptionArguments().size() != 1) {
            throw new UserException(ExitCodes.USAGE, "Exactly one file to analyze must be provided, got [" +
                options.nonOptionArguments().size() + "]");
        }
        Path file = parsePath(options.nonOptionArguments().get(0));
        if (Files.isRegularFile(file) == false) {
            throw new UserException(ExitCodes.USAGE, "[" + file + "] does not exist or is not a file");
        }

        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(1);
        try {
            LogConfigWriter logConfigWriter = new LogConfigWriter(terminal, filebeatModulePath,
                file.toAbsolutePath().normalize().toString(), indexName, typeName, elasticsearchHost, logstashHost, timezone);
            FileStructureFinderManager structureFinderManager = new FileStructureFinderManager(scheduler);
            FileStructureFinder structureFinder = structureFinderManager.findFileStructure(sampleLines, Files.newInputStream(file));
            FileStructure structure = structureFinder.getStructure();
            for (String reason : structure.getExplanation()) {
                terminal.println(Verbosity.VERBOSE, reason);
            }
            logConfigWriter.writeConfigs(structure, structureFinder.getSampleMessages(), outputDirectory);
        } catch (IllegalArgumentException | IOException e) {
            throw new UserException(ExitCodes.DATA_ERROR, "Cannot determine format of file [" + file + "]: " + e.getMessage());
        } finally {
            scheduler.shutdown();
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

    private static void validateHost(String host, String description) throws UserException {
        if (IP_OR_HOST_GROK.match(host) == false) {
            throw new UserException(ExitCodes.USAGE, description + " [" + host + "] is not a valid IP address or hostname");
        }
    }
}
