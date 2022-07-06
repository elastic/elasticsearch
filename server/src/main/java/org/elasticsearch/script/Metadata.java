/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.time.ZonedDateTime;

/**
 * Ingest and update metadata available to write scripts.
 *
 * This interface is a super-set of all metadata for the write contexts.  A write contexts should only
 * whitelist the relevant getter and setters.
 */
public interface Metadata {
    /**
     * The destination index
     */
    String getIndex();

    void setIndex(String index);

    /**
     * The document id
     */
    String getId();

    void setId(String id);

    /**
     * The document routing string
     */
    String getRouting();

    void setRouting(String routing);

    /**
     * The version of the document
     */
    long getVersion();

    void setVersion(long version);

    default boolean hasVersion() {
        throw new UnsupportedOperationException();
    }

    /**
     * The version type of the document, {@link org.elasticsearch.index.VersionType} as a lower-case string.
     * Since update does not hav ethis metadata, defaults to throwing {@link UnsupportedOperationException}.
     */
    default String getVersionType() {
        throw new UnsupportedOperationException();
    }

    /**
     * Set the version type of the document.
     *
     * Since update does not hav ethis metadata, defaults to throwing {@link UnsupportedOperationException}
     * @param versionType {@link org.elasticsearch.index.VersionType} as a lower-case string
     */
    default void setVersionType(String versionType) {
        throw new UnsupportedOperationException();
    }

    /**
     * Get the update operation for this document, eg "create", "index", "noop".
     *
     * Since ingest does not have this metadata, defaults to throwing {@link UnsupportedOperationException}.
     */
    default String getOp() {
        throw new UnsupportedOperationException();
    }

    /**
     * Set the update operation for this document.  See {@code org.elasticsearch.action.update.UpdateHelper.UpdateOpType} and
     * {@code org.elasticsearch.reindex.AbstractAsyncBulkByScrollAction.OpType}
     *
     * Since ingest does not have this metadata, defaults to throwing {@link UnsupportedOperationException}.
     * @param op the op type as a string.
     */
    default void setOp(String op) {
        throw new UnsupportedOperationException();
    }

    /**
     * Timestamp of this ingestion or update.
     */
    ZonedDateTime getTimestamp();
}
