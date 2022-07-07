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
 * Ingest and update metadata available to write scripts
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

    /**
     * The version type of the document, {@link org.elasticsearch.index.VersionType} as a lower-case string.
     */
    String getVersionType();

    /**
     * Set the version type of the document.
     * @param versionType {@link org.elasticsearch.index.VersionType} as a lower-case string
     */
    void setVersionType(String versionType);

    /**
     * Timestamp of this ingestion or update
     */
    ZonedDateTime getTimestamp();
}
