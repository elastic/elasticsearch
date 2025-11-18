/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;

import java.util.List;

/**
 * An IndexMatcher given a {@link Metadata} object, can return a list of index names matching that pattern.
 */
public interface IndexMatcher {
    /**
     * Retrieves a list of all indices which match this descriptor's pattern. Implementations
     * may include other special information when matching indices, such as aliases.
     * <p>
     * This cannot be done via {@link org.elasticsearch.cluster.metadata.IndexNameExpressionResolver} because that class can only handle
     * simple wildcard expressions, but system index name patterns may use full Lucene regular expression syntax.
     *
     * @param project The current metadata to get the list of matching indices from
     * @return A list of index names that match this descriptor
     */
    List<String> getMatchingIndices(ProjectMetadata project);
}
