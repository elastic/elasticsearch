/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.iplocation;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;

/**
 * Processor that tags documents when the IP location database is not yet available.
 * Replaced by a real {@link GeoIpProcessor} when the database becomes available.
 */
public class DatabaseUnavailableProcessor extends AbstractProcessor {

    private final String type;
    private final String databaseName;

    public DatabaseUnavailableProcessor(String type, String tag, String description, String databaseName) {
        super(tag, description);
        this.type = type;
        this.databaseName = databaseName;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        ingestDocument.appendFieldValue("tags", "_" + type + "_database_unavailable_" + databaseName, true);
        return ingestDocument;
    }

    @Override
    public String getType() {
        return type;
    }

    public String getDatabaseName() {
        return databaseName;
    }
}
