/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.iplocation.api.DatabaseAvailabilityListener;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationConsumer;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;
import org.elasticsearch.iplocation.api.IpLocationService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Adapts an {@link IpDatabaseProvider} (used by the logstash-bridge) into
 * an {@link IpLocationService} so that {@code GeoIpProcessor.Factory} can
 * accept bridge-provided databases without change.
 */
public final class IpLocationServiceAdapter implements IpLocationService {

    private static final Logger logger = LogManager.getLogger(IpLocationServiceAdapter.class);

    private final IpDatabaseProvider provider;

    private IpLocationServiceAdapter(IpDatabaseProvider provider) {
        this.provider = provider;
    }

    /**
     * Wraps an {@link IpDatabaseProvider} as an {@link IpLocationService}.
     */
    public static IpLocationService fromDatabaseProvider(IpDatabaseProvider provider) {
        return new IpLocationServiceAdapter(provider);
    }

    @Override
    public IpDataLookup createIpDataLookup(String projectIdStr, String databaseFile, List<String> propertyNames) {
        ProjectId pid = ProjectId.fromId(projectIdStr);
        IpDatabase database = provider.getDatabase(pid, databaseFile);
        if (database == null) {
            return null;
        }
        try {
            String dbType = database.getDatabaseType();
            IpDataLookupFactories.IpDataLookupFactory factory = IpDataLookupFactories.get(dbType, databaseFile);
            InternalIpDataLookup internalLookup = factory.create(propertyNames);
            IpDataLookupInfo info = new IpDataLookupInfoImpl(internalLookup.getProperties(), dbType);
            return new BridgeIpDataLookup(provider, pid, databaseFile, internalLookup, info);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                database.close();
            } catch (IOException e) {
                logger.warn(() -> Strings.format("failed to close database [%s]", databaseFile), e);
            }
        }
    }

    @Override
    public IpDataLookupInfo getIpDataLookupInfo(String databaseFile) {
        // The adapter doesn't have project-agnostic database access, so it falls back
        // to filename-based type guessing. This works for standard databases (MaxMind)
        // but may not resolve non-standard filenames (e.g. ipinfo).
        String dbType = IpDataLookupFactories.guessDatabaseType(databaseFile);
        Database database = IpDataLookupFactories.getDatabase(dbType);
        if (database == null) {
            return null;
        }
        return new IpDataLookupInfoImpl(database.properties(), dbType);
    }

    /**
     * Bridge-provided databases are always available, so there are no availability transitions to notify.
     */
    @Override
    public void addDatabaseAvailabilityListener(DatabaseAvailabilityListener listener) {}

    /**
     * The bridge manages its own database lifecycle; consumer registrations from Elasticsearch ingest
     * code are irrelevant here and are intentionally ignored.
     */
    @Override
    public void requestDownloads(String projectId, IpLocationConsumer consumer) {}

    /**
     * The bridge manages its own database lifecycle; consumer registrations from Elasticsearch ingest
     * code are irrelevant here and are intentionally ignored.
     */
    @Override
    public void cancelDownloadRequest(String projectId, IpLocationConsumer consumer) {}

    /**
     * An {@link IpDataLookup} backed by an {@link IpDatabaseProvider}.
     * Each lookup call obtains a fresh database handle from the provider.
     */
    private static final class BridgeIpDataLookup implements IpDataLookup {

        private final IpDatabaseProvider provider;
        private final ProjectId projectId;
        private final String databaseFile;
        private final InternalIpDataLookup internalLookup;
        private final IpDataLookupInfo info;

        BridgeIpDataLookup(
            IpDatabaseProvider provider,
            ProjectId projectId,
            String databaseFile,
            InternalIpDataLookup internalLookup,
            IpDataLookupInfo info
        ) {
            this.provider = provider;
            this.projectId = projectId;
            this.databaseFile = databaseFile;
            this.internalLookup = internalLookup;
            this.info = info;
        }

        @Override
        public Boolean lookup(String ip, IpLocationInfoCollector collector) throws IOException {
            IpDatabase database = provider.getDatabase(projectId, databaseFile);
            if (database == null) {
                return null;
            }
            try {
                boolean found = internalLookup.getData(database, ip, collector);
                return found;
            } finally {
                database.close();
            }
        }

        @Override
        public boolean isValid() {
            return provider.isValid(projectId, databaseFile);
        }

        @Override
        public IpDataLookupInfo getInfo() {
            return info;
        }
    }
}
