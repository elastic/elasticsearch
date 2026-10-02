/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;

import java.io.IOException;

/**
 * Implementation of the public {@link IpDataLookup} interface.
 * Each instance is a "live handle" to a specific database: each {@link #lookup} call
 * obtains the latest database loader via reference counting.
 */
final class IpDataLookupImpl implements IpDataLookup {

    private final DatabaseNodeService databaseNodeService;
    private final ProjectId projectId;
    private final String databaseFile;
    private final String databaseType;
    private final InternalIpDataLookup internalLookup;
    private final IpDataLookupInfo info;

    IpDataLookupImpl(
        DatabaseNodeService databaseNodeService,
        ProjectId projectId,
        String databaseFile,
        String databaseType,
        InternalIpDataLookup internalLookup,
        IpDataLookupInfo info
    ) {
        this.databaseNodeService = databaseNodeService;
        this.projectId = projectId;
        this.databaseFile = databaseFile;
        this.databaseType = databaseType;
        this.internalLookup = internalLookup;
        this.info = info;
    }

    @Override
    public Boolean lookup(String ip, IpLocationInfoCollector collector) throws IOException {
        try (DatabaseReaderLazyLoader loader = databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseFile)) {
            if (loader == null) {
                return null;
            }
            if (Assertions.ENABLED) {
                verifyDatabaseType(loader);
            }
            return internalLookup.getData(loader, ip, collector);
        }
    }

    @Override
    public boolean isValid() {
        return databaseNodeService.isValid(projectId, databaseFile);
    }

    @Override
    public IpDataLookupInfo getInfo() {
        return info;
    }

    private void verifyDatabaseType(DatabaseReaderLazyLoader loader) throws IOException {
        int last = databaseType.lastIndexOf('-');
        final String expectedSuffix = last == -1 ? null : databaseType.substring(last);
        final String loaderType = loader.getDatabaseType();
        assert loaderType.equals(databaseType) || expectedSuffix == null || loaderType.endsWith(expectedSuffix)
            : "database type [" + loaderType + "] doesn't match with expected suffix [" + expectedSuffix + "]";
    }
}
