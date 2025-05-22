/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.system;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.indices.IndexMatcher;

/**
 * An IndexPatternMatcher holds an index pattern in a string and, given a
 * {@link Metadata} object, can return a list of index names matching that pattern.
 */
public interface IndexPatternMatcher extends IndexMatcher {
    /**
     * @return A pattern, either with a wildcard or simple regex, describing indices that are
     * related to a system feature. Such indices may be system indices or associated
     * indices.
     */
    String getIndexPattern();

}
