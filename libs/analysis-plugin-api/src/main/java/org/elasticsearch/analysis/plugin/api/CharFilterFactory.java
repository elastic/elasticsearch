/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.analysis.plugin.api;

import org.elasticsearch.plugin.api.Nameable;

import java.io.Reader;

/**
 * An analysis component used to create char filters.
 */
public interface CharFilterFactory extends Nameable {
    Reader create(Reader reader);

    default Reader normalize(Reader reader) {
        return reader;
    }

}
