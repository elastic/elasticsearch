/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.structurefinder;

import org.elasticsearch.xpack.core.textstructure.structurefinder.TextStructure;

import java.util.List;
import java.util.regex.Pattern;

public class LogTextStructureFinderFactory implements TextStructureFinderFactory {

    // This works because, by default, dot doesn't match newlines
    private static final Pattern TWO_NON_BLANK_LINES_PATTERN = Pattern.compile(".\n+.");

    @Override
    public boolean canFindFormat(TextStructure.Format format) {
        return format == null || format == TextStructure.Format.SEMI_STRUCTURED_TEXT;
    }

    /**
     * This format matches if the sample contains at least one newline and at least two
     * non-blank lines.
     */
    @Override
    public boolean canCreateFromSample(List<String> explanation, String sample, double allowedFractionOfBadLines) {
        if (sample.indexOf('\n') < 0) {
            explanation.add("Not text because sample contains no newlines");
            return false;
        }
        if (TWO_NON_BLANK_LINES_PATTERN.matcher(sample).find() == false) {
            explanation.add("Not text because sample contains fewer than two non-blank lines");
            return false;
        }

        explanation.add("Deciding sample is text");
        return true;
    }

    @Override
    public TextStructureFinder createFromSample(
        List<String> explanation,
        String sample,
        String charsetName,
        Boolean hasByteOrderMarker,
        int lineMergeSizeLimit,
        TextStructureOverrides overrides,
        TimeoutChecker timeoutChecker
    ) {
        return LogTextStructureFinder.makeLogTextStructureFinder(
            explanation,
            sample,
            charsetName,
            hasByteOrderMarker,
            lineMergeSizeLimit,
            overrides,
            timeoutChecker
        );
    }
}
