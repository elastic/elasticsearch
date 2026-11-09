/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.repository;

/**
 * Where a Parquet file is, in the form that a segment stores: a file below the base path of a snapshot repository
 * ({@link RepositoryLocation}) or a URI that an ES|QL data source reads ({@link DataSourceLocation}). The string form names the kind in
 * its scheme, so a segment that was attached years ago still says how to open its file.
 */
public sealed interface FileLocation permits RepositoryLocation, DataSourceLocation {

    /** The name of the repository or of the data source, for messages. */
    String sourceName();

    /** The stored form, which {@link #parse} reads back. */
    @Override
    String toString();

    static FileLocation parse(String location) {
        if (location.startsWith(RepositoryLocation.SCHEME)) {
            return RepositoryLocation.parse(location);
        }
        if (location.startsWith(DataSourceLocation.SCHEME)) {
            return DataSourceLocation.parse(location);
        }
        throw new IllegalArgumentException("not a file location [" + location + "]");
    }
}
