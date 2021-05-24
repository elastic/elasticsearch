/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.Metadata;

import java.util.List;

/**
 * An IndexDescriptor is an object that
 */
public interface IndexDescriptor {
    /**
     * @return A pattern, either with a wildcard or simple regex, describing indices that are
     * related to a system feature. Such indices may be system indices or associated
     * indices.
     */
    String getIndexPattern();

    /**
     * @return A text description of the purpose of the indices covered by this descriptor.
     */
    String getDescription();

    /**
     * Retrieves a list of all indices which match this descriptor's pattern. Implementations
     * may include other special information when matching indices, such as aliases.
     *
     * This cannot be done via {@link org.elasticsearch.cluster.metadata.IndexNameExpressionResolver} because that class can only handle
     * simple wildcard expressions, but system index name patterns may use full Lucene regular expression syntax,
     *
     * @param metadata The current metadata to get the list of matching indices from
     * @return A list of index names that match this descriptor
     */
    List<String> getMatchingIndices(Metadata metadata);
}
