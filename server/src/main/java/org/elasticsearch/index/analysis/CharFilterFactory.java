/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.analysis;

import java.io.Reader;

/**
 * Factory interface for creating character filters in the analysis chain.
 * Character filters process the input text before tokenization, performing operations
 * such as HTML stripping, pattern replacement, or character mapping.
 */
public interface CharFilterFactory {

    /**
     * Retrieves the name of this character filter factory.
     *
     * @return the character filter name
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CharFilterFactory factory = ...;
     * String name = factory.name(); // e.g., "html_strip"
     * }</pre>
     */
    String name();

    /**
     * Creates a character filter that wraps the provided reader.
     * This method is called during the analysis process to build the character filtering chain.
     *
     * @param reader the input reader to filter
     * @return a new Reader that filters the input
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CharFilterFactory factory = ...;
     * Reader input = new StringReader("<html>text</html>");
     * Reader filtered = factory.create(input);
     * }</pre>
     */
    Reader create(Reader reader);

    /**
     * Normalizes a reader for use in multi-term queries.
     * The default implementation returns the reader unchanged.
     *
     * @param reader the input reader to normalize
     * @return a normalized Reader, by default the same reader
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * CharFilterFactory factory = ...;
     * Reader input = new StringReader("text");
     * Reader normalized = factory.normalize(input);
     * }</pre>
     */
    default Reader normalize(Reader reader) {
        return reader;
    }
}
