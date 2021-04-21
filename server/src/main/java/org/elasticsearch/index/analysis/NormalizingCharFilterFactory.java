/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.analysis;

import java.io.Reader;

/**
 * A CharFilterFactory that also supports normalization
 *
 * The default implementation of {@link #normalize(Reader)} delegates to
 * {@link #create(Reader)}
 */
public interface NormalizingCharFilterFactory extends CharFilterFactory {

    @Override
    default Reader normalize(Reader reader) {
        return create(reader);
    }

}
