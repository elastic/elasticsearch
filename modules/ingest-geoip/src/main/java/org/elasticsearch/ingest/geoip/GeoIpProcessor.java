/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.Database.Property;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class GeoIpProcessor extends AbstractProcessor {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GeoIpProcessor.class);
    static final String DEFAULT_DATABASES_DEPRECATION_MESSAGE = "the [fallback_to_default_databases] has been deprecated, because "
        + "Elasticsearch no longer includes the default Maxmind geoip databases. This setting will be removed in Elasticsearch 9.0";

    public static final String TYPE = "geoip";

    private final String field;
    private final Supplier<Boolean> isValid;
    private final String targetField;
    private final CheckedSupplier<GeoIpDatabase, IOException> supplier;
    private final GeoDataLookup geoDataLookup;
    private final boolean ignoreMissing;
    private final boolean firstOnly;
    private final String databaseFile;

    /**
     * Construct a geo-IP processor.
     * @param tag           the processor tag
     * @param description   the processor description
     * @param field         the source field to geo-IP map
     * @param supplier      a supplier of a geo-IP database reader; ideally this is lazily-loaded once on first use
     * @param isValid       a supplier that determines if the available database files are up-to-date and license compliant
     * @param targetField   the target field
     * @param geoDataLookup a lookup capable of retrieving a result from an available geo-IP database reader
     * @param ignoreMissing true if documents with a missing value for the field should be ignored
     * @param firstOnly     true if only first result should be returned in case of array
     * @param databaseFile  the name of the database file being queried; used only for tagging documents if the database is unavailable
     */
    GeoIpProcessor(
        final String tag,
        final String description,
        final String field,
        final CheckedSupplier<GeoIpDatabase, IOException> supplier,
        final Supplier<Boolean> isValid,
        final String targetField,
        final GeoDataLookup geoDataLookup,
        final boolean ignoreMissing,
        final boolean firstOnly,
        final String databaseFile
    ) {
        super(tag, description);
        this.field = field;
        this.isValid = isValid;
        this.targetField = targetField;
        this.supplier = supplier;
        this.geoDataLookup = geoDataLookup;
        this.ignoreMissing = ignoreMissing;
        this.firstOnly = firstOnly;
        this.databaseFile = databaseFile;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws IOException {
        Object ip = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);

        if (isValid.get() == false) {
            ingestDocument.appendFieldValue("tags", "_geoip_expired_database", false);
            return ingestDocument;
        } else if (ip == null && ignoreMissing) {
            return ingestDocument;
        } else if (ip == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract geoip information.");
        }

        GeoIpDatabase geoIpDatabase = this.supplier.get();
        if (geoIpDatabase == null) {
            if (ignoreMissing == false) {
                tag(ingestDocument, databaseFile);
            }
            return ingestDocument;
        }

        try {
            if (ip instanceof String ipString) {
                Map<String, Object> geoData = getGeoData(geoIpDatabase, ipString);
                if (geoData.isEmpty() == false) {
                    ingestDocument.setFieldValue(targetField, geoData);
                }
            } else if (ip instanceof List<?> ipList) {
                boolean match = false;
                List<Map<String, Object>> geoDataList = new ArrayList<>(ipList.size());
                for (Object ipAddr : ipList) {
                    if (ipAddr instanceof String == false) {
                        throw new IllegalArgumentException("array in field [" + field + "] should only contain strings");
                    }
                    Map<String, Object> geoData = getGeoData(geoIpDatabase, (String) ipAddr);
                    if (geoData.isEmpty()) {
                        geoDataList.add(null);
                        continue;
                    }
                    if (firstOnly) {
                        ingestDocument.setFieldValue(targetField, geoData);
                        return ingestDocument;
                    }
                    match = true;
                    geoDataList.add(geoData);
                }
                if (match) {
                    ingestDocument.setFieldValue(targetField, geoDataList);
                }
            } else {
                throw new IllegalArgumentException("field [" + field + "] should contain only string or array of strings");
            }
        } finally {
            geoIpDatabase.release();
        }
        return ingestDocument;
    }

    private Map<String, Object> getGeoData(GeoIpDatabase geoIpDatabase, String ip) throws IOException {
        return geoDataLookup.getGeoData(geoIpDatabase, InetAddresses.forString(ip));
    }

    @Override
    public String getType() {
        return TYPE;
    }

    String getField() {
        return field;
    }

    String getTargetField() {
        return targetField;
    }

    String getDatabaseType() throws IOException {
        return supplier.get().getDatabaseType();
    }

    Set<Property> getProperties() {
        return geoDataLookup.getProperties();
    }

    /**
     * Retrieves and verifies a {@link GeoIpDatabase} instance for each execution of the {@link GeoIpProcessor}. Guards against missing
     * custom databases, and ensures that database instances are of the proper type before use.
     */
    public static final class DatabaseVerifyingSupplier implements CheckedSupplier<GeoIpDatabase, IOException> {
        private final GeoIpDatabaseProvider geoIpDatabaseProvider;
        private final String databaseFile;
        private final String databaseType;

        public DatabaseVerifyingSupplier(GeoIpDatabaseProvider geoIpDatabaseProvider, String databaseFile, String databaseType) {
            this.geoIpDatabaseProvider = geoIpDatabaseProvider;
            this.databaseFile = databaseFile;
            this.databaseType = databaseType;
        }

        @Override
        public GeoIpDatabase get() throws IOException {
            GeoIpDatabase loader = geoIpDatabaseProvider.getDatabase(databaseFile);
            if (loader == null) {
                return null;
            }

            if (Assertions.ENABLED) {
                // Only check whether the suffix has changed and not the entire database type.
                // To sanity check whether a city db isn't overwriting with a country or asn db.
                // For example overwriting a geoip lite city db with geoip city db is a valid change, but the db type is slightly different,
                // by checking just the suffix this assertion doesn't fail.
                String expectedSuffix = databaseType.substring(databaseType.lastIndexOf('-'));
                assert loader.getDatabaseType().endsWith(expectedSuffix)
                    : "database type [" + loader.getDatabaseType() + "] doesn't match with expected suffix [" + expectedSuffix + "]";
            }
            return loader;
        }
    }

    public static final class Factory implements Processor.Factory {

        private final GeoIpDatabaseProvider geoIpDatabaseProvider;

        public Factory(GeoIpDatabaseProvider geoIpDatabaseProvider) {
            this.geoIpDatabaseProvider = geoIpDatabaseProvider;
        }

        @Override
        public Processor create(
            final Map<String, Processor.Factory> registry,
            final String processorTag,
            final String description,
            final Map<String, Object> config
        ) throws IOException {
            String ipField = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "geoip");
            String databaseFile = readStringProperty(TYPE, processorTag, config, "database_file", "GeoLite2-City.mmdb");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean firstOnly = readBooleanProperty(TYPE, processorTag, config, "first_only", true);

            // Validating the download_database_on_pipeline_creation even if the result
            // is not used directly by the factory.
            downloadDatabaseOnPipelineCreation(config, processorTag);

            // noop, should be removed in 9.0
            Object value = config.remove("fallback_to_default_databases");
            if (value != null) {
                deprecationLogger.warn(DeprecationCategory.OTHER, "default_databases_message", DEFAULT_DATABASES_DEPRECATION_MESSAGE);
            }

            GeoIpDatabase geoIpDatabase = geoIpDatabaseProvider.getDatabase(databaseFile);
            if (geoIpDatabase == null) {
                // It's possible that the database could be downloaded via the GeoipDownloader process and could become available
                // at a later moment, so a processor impl is returned that tags documents instead. If a database cannot be sourced then the
                // processor will continue to tag documents with a warning until it is remediated by providing a database or changing the
                // pipeline.
                return new DatabaseUnavailableProcessor(processorTag, description, databaseFile);
            }

            final String databaseType;
            try {
                databaseType = geoIpDatabase.getDatabaseType();
            } finally {
                geoIpDatabase.release();
            }

            final Database database;
            try {
                database = Database.getDatabase(databaseType, databaseFile);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, "database_file", e.getMessage());
            }

            final Set<Property> properties;
            try {
                properties = database.parseProperties(propertyNames);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, "properties", e.getMessage());
            }

            final GeoDataLookup geoDataLookup = GeoDataLookupFactory.get(database).create(properties);

            return new GeoIpProcessor(
                processorTag,
                description,
                ipField,
                new DatabaseVerifyingSupplier(geoIpDatabaseProvider, databaseFile, databaseType),
                () -> geoIpDatabaseProvider.isValid(databaseFile),
                targetField,
                geoDataLookup,
                ignoreMissing,
                firstOnly,
                databaseFile
            );
        }

        public static boolean downloadDatabaseOnPipelineCreation(Map<String, Object> config) {
            return downloadDatabaseOnPipelineCreation(config, null);
        }

        public static boolean downloadDatabaseOnPipelineCreation(Map<String, Object> config, String processorTag) {
            return readBooleanProperty(GeoIpProcessor.TYPE, processorTag, config, "download_database_on_pipeline_creation", true);
        }

    }

    static class DatabaseUnavailableProcessor extends AbstractProcessor {

        private final String databaseName;

        DatabaseUnavailableProcessor(String tag, String description, String databaseName) {
            super(tag, description);
            this.databaseName = databaseName;
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            tag(ingestDocument, databaseName);
            return ingestDocument;
        }

        @Override
        public String getType() {
            return TYPE;
        }

        public String getDatabaseName() {
            return databaseName;
        }
    }

    private static void tag(IngestDocument ingestDocument, String databaseName) {
        ingestDocument.appendFieldValue("tags", "_geoip_database_unavailable_" + databaseName, true);
    }
}
