/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * The list of paths where a blob can reside.  The contents of the paths are dependent upon the implementation of {@link BlobContainer}.
 */
public class BlobPath {

    public static final BlobPath EMPTY = new BlobPath(Collections.emptyList());

    private static final String SEPARATOR = "/";

    private final List<String> paths;

    private BlobPath(List<String> paths) {
        this.paths = paths;
    }

    public List<String> parts() {
        return paths;
    }

    public BlobPath add(String path) {
        return new BlobPath(
            CollectionUtils.appendToCopy(this.paths, path.endsWith(SEPARATOR) ? path.substring(0, path.length() - 1) : path));
    }

    public String buildAsString() {
        String p = String.join(SEPARATOR, paths);
        if (p.isEmpty()) {
            return p;
        }
        return p + SEPARATOR;
    }

    /**
     * Returns this path's parent path.
     *
     * @return Parent path or {@code null} if there is none
     */
    @Nullable
    public BlobPath parent() {
        int size = paths.size();
        switch (size) {
            case 0:
                return null;
            case 1:
                return EMPTY;
            default:
                return new BlobPath(List.copyOf(paths.subList(0, size - 1)));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String path : paths) {
            sb.append('[').append(path).append(']');
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BlobPath other = (BlobPath) o;
        return paths.equals(other.paths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(paths);
    }
}
