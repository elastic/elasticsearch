/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import java.util.ArrayList;
import java.util.List;

/**
 * Where a Parquet file is: a registered repository and a path below the base path of that repository. Keeping the file below the base
 * path means that a mount cannot read anything that the repository was not set up to expose.
 *
 * @param repository the name of the repository
 * @param path the path of the file relative to the base path of the repository, with {@code /} between the segments
 */
public record RepositoryLocation(String repository, String path) {

    private static final String SCHEME = "repository://";

    public RepositoryLocation {
        if (repository.isEmpty() || repository.contains("/")) {
            throw new IllegalArgumentException("invalid repository name [" + repository + "]");
        }
        segments(path);
    }

    /** The segments of the path, without validation errors being swallowed: an empty segment, {@code .} or {@code ..} is rejected. */
    public List<String> segments() {
        return segments(path);
    }

    private static List<String> segments(String path) {
        List<String> segments = new ArrayList<>();
        for (String segment : path.split("/", -1)) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("invalid path [" + path + "], it must stay below the base path of the repository");
            }
            segments.add(segment);
        }
        return segments;
    }

    @Override
    public String toString() {
        return SCHEME + repository + "/" + path;
    }

    public static RepositoryLocation parse(String location) {
        if (location.startsWith(SCHEME) == false) {
            throw new IllegalArgumentException("not a repository location [" + location + "]");
        }
        String rest = location.substring(SCHEME.length());
        int slash = rest.indexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("not a repository location [" + location + "]");
        }
        return new RepositoryLocation(rest.substring(0, slash), rest.substring(slash + 1));
    }
}
