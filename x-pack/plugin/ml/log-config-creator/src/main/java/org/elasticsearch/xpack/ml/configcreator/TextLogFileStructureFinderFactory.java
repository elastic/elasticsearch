/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.configcreator;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;

import java.util.Objects;
import java.util.regex.Pattern;

public class TextLogFileStructureFinderFactory implements LogFileStructureFinderFactory {

    // This works because, by default, dot doesn't match newlines
    private static final Pattern TWO_NON_BLANK_LINES_PATTERN = Pattern.compile(".\n+.");

    private final Terminal terminal;

    public TextLogFileStructureFinderFactory(Terminal terminal) {
        this.terminal = Objects.requireNonNull(terminal);
    }

    /**
     * This format matches if the sample contains at least one newline and at least two
     * non-blank lines.
     */
    @Override
    public boolean canCreateFromSample(String sample) {
        if (sample.indexOf('\n') < 0) {
            terminal.println(Verbosity.VERBOSE, "Not text because sample contains no newlines");
            return false;
        }
        if (TWO_NON_BLANK_LINES_PATTERN.matcher(sample).find() == false) {
            terminal.println(Verbosity.VERBOSE, "Not text because sample contains fewer than two non-blank lines");
            return false;
        }

        terminal.println(Verbosity.VERBOSE, "Deciding sample is text");
        return true;
    }

    @Override
    public LogFileStructureFinder createFromSample(String sample, String charsetName, Boolean hasByteOrderMarker) throws UserException {
        return new TextLogFileStructureFinder(terminal, sample, charsetName, hasByteOrderMarker);
    }
}
