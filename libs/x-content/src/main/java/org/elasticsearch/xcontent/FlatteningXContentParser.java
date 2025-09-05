/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import java.io.IOException;

/**
 * A subclass of XContentSubParser that provides the functionality to flatten
 * the field names by prefixing them with the provided parent name.
 */
public class FlatteningXContentParser extends XContentSubParser {
    private final String parentName;
    private static final char DELIMITER = '.';

    /**
     * Constructs a FlatteningXContentParser with the given parent name and wraps an existing XContentParser.
     *
     * @param parser The XContentParser to be wrapped and extended with flattening functionality.
     * @param parentName The parent name to be used as a prefix for immediate children.
     */
    public FlatteningXContentParser(XContentParser parser, String parentName) {
        super(parser);
        this.parentName = parentName;
    }

    /**
     * Retrieves the name of the current field being parsed. If the current parsing level is 1,
     * the returned field name will be constructed by prepending the parent name to the
     * delegate's currentFieldName, otherwise just delegate.
     *
     * @return The current field name, potentially modified by prepending the parent name as a prefix.
     * @throws IOException If an I/O error occurs during parsing.
     */
    @Override
    public String currentName() throws IOException {
        if (level() == 1) {
            return new StringBuilder(parentName).append(DELIMITER).append(delegate().currentName()).toString();
        }
        return delegate().currentName();
    }
}
