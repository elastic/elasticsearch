/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.repository;

import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

/**
 * A file that an ES|QL data source reads: the name of the data source, which holds the type and the settings (the authentication, the
 * endpoint), and the URI of the file, for example {@code s3://bucket/path/file.parquet}. Unlike a repository, a data source has no base
 * path, so the URI is complete.
 *
 * @param dataSource the name of the data source
 * @param uri        the URI of the file, with a scheme that the data source reads
 */
public record DataSourceLocation(String dataSource, String uri) implements FileLocation {

    static final String SCHEME = "datasource://";

    public DataSourceLocation {
        if (dataSource.isEmpty() || dataSource.contains("/")) {
            throw new IllegalArgumentException("invalid data source name [" + dataSource + "]");
        }
        // a piece of an index is written as uri#firstRowGroup+count, so the URI must not contain a #
        if (uri.contains("#")) {
            throw new IllegalArgumentException("invalid path [" + uri + "], it must not contain a #");
        }
        StoragePath path;
        try {
            path = StoragePath.of(uri);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid path [" + uri + "], it must be a URI such as s3://bucket/file.parquet", e);
        }
        if (path.scheme() == null || path.scheme().isEmpty() || uri.contains("..")) {
            throw new IllegalArgumentException("invalid path [" + uri + "], it must be a URI such as s3://bucket/file.parquet");
        }
    }

    public StoragePath storagePath() {
        return StoragePath.of(uri);
    }

    @Override
    public String sourceName() {
        return dataSource;
    }

    @Override
    public String toString() {
        return SCHEME + dataSource + "/" + uri;
    }

    public static DataSourceLocation parse(String location) {
        if (location.startsWith(SCHEME) == false) {
            throw new IllegalArgumentException("not a data source location [" + location + "]");
        }
        String rest = location.substring(SCHEME.length());
        int slash = rest.indexOf('/');
        if (slash < 1) {
            throw new IllegalArgumentException("not a data source location [" + location + "]");
        }
        return new DataSourceLocation(rest.substring(0, slash), rest.substring(slash + 1));
    }
}
