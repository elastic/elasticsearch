/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.Database.Property;
import org.elasticsearch.ingest.geoip.IpDataLookupFactories.IpDataLookupFactory;

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
    private final CheckedSupplier<IpDatabase, IOException> supplier;
    private final IpDataLookup ipDataLookup;
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
     * @param ipDataLookup a lookup capable of retrieving a result from an available geo-IP database reader
     * @param ignoreMissing true if documents with a missing value for the field should be ignored
     * @param firstOnly     true if only first result should be returned in case of array
     * @param databaseFile  the name of the database file being queried; used only for tagging documents if the database is unavailable
     */
    GeoIpProcessor(
        final String tag,
        final String description,
        final String field,
        final CheckedSupplier<IpDatabase, IOException> supplier,
        final Supplier<Boolean> isValid,
        final String targetField,
        final IpDataLookup ipDataLookup,
        final boolean ignoreMissing,
        final boolean firstOnly,
        final String databaseFile
    ) {
        super(tag, description);
        this.field = field;
        this.isValid = isValid;
        this.targetField = targetField;
        this.supplier = supplier;
        this.ipDataLookup = ipDataLookup;
        this.ignoreMissing = ignoreMissing;
        this.firstOnly = firstOnly;
        this.databaseFile = databaseFile;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws IOException {
        Object ip = document.getFieldValue(field, Object.class, ignoreMissing);

        if (isValid.get() == false) {
            document.appendFieldValue("tags", "_geoip_expired_database", false);
            return document;
        } else if (ip == null && ignoreMissing) {
            return document;
        } else if (ip == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract geoip information.");
        }

        try (IpDatabase ipDatabase = this.supplier.get()) {
            if (ipDatabase == null) {
                if (ignoreMissing == false) {
                    tag(document, databaseFile);
                }
                return document;
            }

            if (ip instanceof String ipString) {
                Map<String, Object> data = ipDataLookup.getData(ipDatabase, ipString);
                if (data.isEmpty() == false) {
                    document.setFieldValue(targetField, data);
                }
            } else if (ip instanceof List<?> ipList) {
                boolean match = false;
                List<Map<String, Object>> dataList = new ArrayList<>(ipList.size());
                for (Object ipAddr : ipList) {
                    if (ipAddr instanceof String == false) {
                        throw new IllegalArgumentException("array in field [" + field + "] should only contain strings");
                    }
                    Map<String, Object> data = ipDataLookup.getData(ipDatabase, (String) ipAddr);
                    if (data.isEmpty()) {
                        dataList.add(null);
                        continue;
                    }
                    if (firstOnly) {
                        document.setFieldValue(targetField, data);
                        return document;
                    }
                    match = true;
                    dataList.add(data);
                }
                if (match) {
                    document.setFieldValue(targetField, dataList);
                }
            } else {
                throw new IllegalArgumentException("field [" + field + "] should contain only string or array of strings");
            }
        }

        return document;
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
        return ipDataLookup.getProperties();
    }

    /**
     * Retrieves and verifies a {@link IpDatabase} instance for each execution of the {@link GeoIpProcessor}. Guards against missing
     * custom databases, and ensures that database instances are of the proper type before use.
     */
    public static final class DatabaseVerifyingSupplier implements CheckedSupplier<IpDatabase, IOException> {
        private final IpDatabaseProvider ipDatabaseProvider;
        private final String databaseFile;
        private final String databaseType;

        public DatabaseVerifyingSupplier(IpDatabaseProvider ipDatabaseProvider, String databaseFile, String databaseType) {
            this.ipDatabaseProvider = ipDatabaseProvider;
            this.databaseFile = databaseFile;
            this.databaseType = databaseType;
        }

        @Override
        public IpDatabase get() throws IOException {
            IpDatabase loader = ipDatabaseProvider.getDatabase(databaseFile);
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

        private final IpDatabaseProvider ipDatabaseProvider;

        public Factory(IpDatabaseProvider ipDatabaseProvider) {
            this.ipDatabaseProvider = ipDatabaseProvider;
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

            final String databaseType;
            try (IpDatabase ipDatabase = ipDatabaseProvider.getDatabase(databaseFile)) {
                if (ipDatabase == null) {
                    // It's possible that the database could be downloaded via the GeoipDownloader process and could become available
                    // at a later moment, so a processor impl is returned that tags documents instead. If a database cannot be sourced
                    // then the processor will continue to tag documents with a warning until it is remediated by providing a database
                    // or changing the pipeline.
                    return new DatabaseUnavailableProcessor(processorTag, description, databaseFile);
                }
                databaseType = ipDatabase.getDatabaseType();
            }

            final IpDataLookupFactory factory;
            try {
                factory = IpDataLookupFactories.get(databaseType, databaseFile);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, "database_file", e.getMessage());
            }

            final IpDataLookup ipDataLookup;
            try {
                ipDataLookup = factory.create(propertyNames);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(TYPE, processorTag, "properties", e.getMessage());
            }

            return new GeoIpProcessor(
                processorTag,
                description,
                ipField,
                new DatabaseVerifyingSupplier(ipDatabaseProvider, databaseFile, databaseType),
                () -> ipDatabaseProvider.isValid(databaseFile),
                targetField,
                ipDataLookup,
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
