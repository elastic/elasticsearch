/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugin.analysis;

import org.elasticsearch.plugin.Extensible;
import org.elasticsearch.plugin.Nameable;

import java.io.Reader;

/**
 * An analysis component used to create char filters.
 *
 */
@Extensible
public interface CharFilterFactory extends Nameable {
    /**
     * Wraps the given Reader with a CharFilter.
     * @param reader reader to be wrapped
     * @return a reader wrapped with CharFilter
     */
    Reader create(Reader reader);

    /**
     * Normalize a tokenStream for use in multi-term queries.
     * The default implementation returns the given reader.
     */
    default Reader normalize(Reader reader) {
        return reader;
    }
}
