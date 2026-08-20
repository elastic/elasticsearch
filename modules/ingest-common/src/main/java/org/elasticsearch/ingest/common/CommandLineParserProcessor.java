/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.common.CommandLineParser.ShellType;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that parses a command line string into a list of individual
 * arguments following shell-specific quoting and escaping rules. This is
 * useful for security event processing where command lines recorded from
 * process events need to be split into the equivalent of the
 * {@code process.args} ECS field.
 */
public final class CommandLineParserProcessor extends AbstractProcessor {

    public static final String TYPE = "command_line_args";

    private final String field;
    private final String targetField;
    private final ShellType shell;
    private final boolean ignoreMissing;

    CommandLineParserProcessor(String tag, String description, String field, String targetField, ShellType shell, boolean ignoreMissing) {
        super(tag, description);
        this.field = field;
        this.targetField = targetField;
        this.shell = shell;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        String commandLine = document.getFieldValue(field, String.class, ignoreMissing);

        if (commandLine == null && ignoreMissing) {
            return document;
        } else if (commandLine == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot parse command line.");
        }

        List<String> args = CommandLineParser.parse(commandLine, shell);
        document.setFieldValue(targetField, args);
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    ShellType getShell() {
        return shell;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    /**
     * Parses a command line string into a list of arguments. This method is
     * exposed for use by the {@link Processors} Painless bridge.
     *
     * @param commandLine the command line to parse
     * @param shell       the shell type name (e.g. "bash", "cmd", "powershell")
     * @return a list of parsed arguments
     */
    static List<String> apply(String commandLine, String shell) {
        return CommandLineParser.parse(commandLine, ShellType.fromString(shell));
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public CommandLineParserProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String field = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "field");
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", field);
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            String shellStr = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "shell", "bash");
            ShellType shell;
            try {
                shell = ShellType.fromString(shellStr);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, "shell", e.getMessage());
            }
            return new CommandLineParserProcessor(processorTag, description, field, targetField, shell, ignoreMissing);
        }
    }
}
