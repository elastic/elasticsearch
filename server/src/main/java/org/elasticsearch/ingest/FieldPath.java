/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Map;

final class FieldPath {

    private static final int MAX_SIZE = 512;
    private static final Map<String, FieldPath> CACHE = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    // constructing a new FieldPath requires that we parse a String (e.g. "foo.bar.baz") into an array
    // of path elements (e.g. ["foo", "bar", "baz"]). Calling String#split results in the allocation
    // of an ArrayList to hold the results, then a new String is created for each path element, and
    // then finally a String[] is allocated to hold the actual result -- in addition to all that, we
    // do some processing ourselves on the path and path elements to validate and prepare them.
    // the above CACHE and the below 'FieldPath.of' method allow us to almost always avoid this work.
    //
    // note that parsing is independent of the access pattern: the only thing that depends on the access pattern is whether
    // the parsed path is valid, and that is checked (cheaply) on every call to 'of' rather than being baked into the cache.

    static FieldPath of(String path, IngestPipelineFieldAccessPattern accessPattern) {
        if (Strings.isEmpty(path)) {
            throw new IllegalArgumentException("path cannot be null nor empty");
        }
        FieldPath res = CACHE.get(path);
        if (res == null) {
            res = new FieldPath(path);
            if (CACHE.size() > MAX_SIZE) {
                CACHE.clear();
            }
            CACHE.put(path, res);
        }
        res.validate(path, accessPattern);
        return res;
    }

    final String[] pathElements;
    private final boolean useIngestContext;
    private final boolean containsBrackets;

    // you shouldn't call this directly, use the FieldPath.of method above instead!
    private FieldPath(String path) {
        String newPath;
        if (path.startsWith(IngestDocument.INGEST_PREFIX)) {
            useIngestContext = true;
            newPath = path.substring(IngestDocument.INGEST_PREFIX.length());
        } else {
            useIngestContext = false;
            if (path.startsWith(IngestDocument.SOURCE_PREFIX)) {
                newPath = path.substring(IngestDocument.SOURCE_PREFIX.length());
            } else {
                newPath = path;
            }
        }
        String[] pathParts = newPath.split("\\.");
        if (pathParts.length == 0) {
            // String#split drops trailing empty strings, so a path made up of nothing but separators (e.g. "." or "_source..")
            // yields no elements at all, which is just as invalid as an empty element in the beginning or middle of a path
            throw new IllegalArgumentException("path [" + path + "] is not valid");
        }
        boolean brackets = false;
        for (String pathPart : pathParts) {
            // empty path elements are not valid in any access pattern
            if (pathPart.isEmpty()) {
                throw new IllegalArgumentException("path [" + path + "] is not valid");
            }
            brackets |= pathPart.contains("[") || pathPart.contains("]");
        }
        this.pathElements = pathParts;
        this.containsBrackets = brackets;
    }

    /**
     * Checks the access pattern specific rules for a parsed path. The {@link IngestPipelineFieldAccessPattern#CLASSIC} pattern
     * supports syntax like context aware array access, so it has no further restrictions. The
     * {@link IngestPipelineFieldAccessPattern#FLEXIBLE} pattern reserves square brackets for (future) array access syntax, so
     * path elements containing them are rejected.
     * @param fullPath The un-split path to use for error messages
     * @param accessPattern The access pattern to validate against
     * @throws IllegalArgumentException if this path is not valid for the access pattern
     */
    private void validate(String fullPath, IngestPipelineFieldAccessPattern accessPattern) {
        switch (accessPattern) {
            case CLASSIC -> {
            }
            case FLEXIBLE -> {
                if (containsBrackets) {
                    throw new IllegalArgumentException("path [" + fullPath + "] is not valid");
                }
            }
        }
    }

    Object initialContext(IngestDocument document) {
        return useIngestContext ? document.getIngestMetadata() : document.getCtxMap();
    }
}
