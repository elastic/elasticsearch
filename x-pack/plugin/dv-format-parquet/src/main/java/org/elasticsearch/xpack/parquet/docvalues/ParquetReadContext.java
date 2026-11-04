/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.CheckedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.repositories.RepositoriesService;

import java.util.function.Supplier;

/**
 * Hands the files to the segments that are opened. Lucene instantiates a doc values format through SPI, without any context, so
 * a segment cannot ask for the repositories of its node. The engine that opens the segments runs the open inside
 * {@link #with(ParquetObjects, CheckedSupplier)}, and the producer of each segment keeps what it finds. A segment that is opened
 * anywhere else, for example by a tool that does not know about these files, can still be inspected but cannot read a value.
 */
public final class ParquetReadContext {

    private static final ThreadLocal<ParquetObjects> CURRENT = new ThreadLocal<>();

    private ParquetReadContext() {}

    /** The files that segments opened on this thread read from, or {@code null} outside of {@link #with}. */
    @Nullable
    static ParquetObjects current() {
        return CURRENT.get();
    }

    /** Runs the supplier with the given files for every segment that it opens. */
    public static <T, E extends Exception> T with(ParquetObjects objects, CheckedSupplier<T, E> supplier) throws E {
        ParquetObjects previous = CURRENT.get();
        CURRENT.set(objects);
        try {
            return supplier.get();
        } finally {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    /** The files of the registered repositories of a node. */
    static ParquetObjects repositories(Supplier<RepositoriesService> repositories, ProjectId projectId) {
        return (location, length) -> RepositoryParquetObject.open(
            repositories.get(),
            projectId,
            RepositoryLocation.parse(location),
            length
        );
    }
}
