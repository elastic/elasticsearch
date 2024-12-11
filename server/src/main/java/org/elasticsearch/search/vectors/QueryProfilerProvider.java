/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.KnnFloatVectorField;
import org.elasticsearch.search.profile.query.QueryProfiler;

/**
 *
 * <p> This interface includes the declaration of an abstract method, profile(). Classes implementing this interface
 * must provide an implementation for profile() to store profiling information in the {@link QueryProfiler}.
 */

public interface QueryProfilerProvider {

    /**
     * Store the profiling information in the {@link QueryProfiler}
     * @param queryProfiler an instance of  {@link KnnFloatVectorField}.
     */
    void profile(QueryProfiler queryProfiler);
}
