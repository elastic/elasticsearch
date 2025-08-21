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
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Strings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.Database.Property;
import org.elasticsearch.ingest.geoip.IpDataLookupFactories.IpDataLookupFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class GeoIpProcessor extends AbstractProcessor {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GeoIpProcessor.class);
    static final String UNSUPPORTED_DATABASE_DEPRECATION_MESSAGE = "the geoip processor will no longer support database type [{}] "
        + "in a future version of Elasticsearch"; // TODO add a message about migration?

    public static final String GEOIP_TYPE = "geoip";
    public static final String IP_LOCATION_TYPE = "ip_location";

    private final String type;
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
        final String type,
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
        this.type = type;
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
            document.appendFieldValue("tags", "_" + type + "_expired_database", false);
            return document;
        } else if (ip == null && ignoreMissing) {
            return document;
        } else if (ip == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract geoip information.");
        }

        try (IpDatabase ipDatabase = this.supplier.get()) {
            if (ipDatabase == null) {
                if (ignoreMissing == false) {
                    tag(document, type, databaseFile);
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
        return type;
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
        private final ProjectId projectId;

        public DatabaseVerifyingSupplier(
            IpDatabaseProvider ipDatabaseProvider,
            String databaseFile,
            String databaseType,
            ProjectId projectId
        ) {
            this.ipDatabaseProvider = ipDatabaseProvider;
            this.databaseFile = databaseFile;
            this.databaseType = databaseType;
            this.projectId = projectId;
        }

        @Override
        public IpDatabase get() throws IOException {
            IpDatabase loader = ipDatabaseProvider.getDatabase(projectId, databaseFile);
            if (loader == null) {
                return null;
            }

            if (Assertions.ENABLED) {
                // Note that the expected suffix might be null for providers that aren't amenable to using dashes as separator for
                // determining the database type.
                int last = databaseType.lastIndexOf('-');
                final String expectedSuffix = last == -1 ? null : databaseType.substring(last);

                // If the entire database type matches, then that's a match. Otherwise, if there's a suffix to compare on, then
                // check whether the suffix has changed (not the entire database type).
                // This is to sanity check, for example, that a city db isn't overwritten with a country or asn db.
                // But there are permissible overwrites that make sense, for example overwriting a geolite city db with a geoip city db
                // is a valid change, but the db type is slightly different -- by checking just the suffix this assertion won't fail.
                final String loaderType = loader.getDatabaseType();
                assert loaderType.equals(databaseType) || expectedSuffix == null || loaderType.endsWith(expectedSuffix)
                    : "database type [" + loaderType + "] doesn't match with expected suffix [" + expectedSuffix + "]";
            }
            return loader;
        }
    }

    public static final class Factory implements Processor.Factory {

        private final String type; // currently always just "geoip"
        private final IpDatabaseProvider ipDatabaseProvider;

        public Factory(String type, IpDatabaseProvider ipDatabaseProvider) {
            this.type = type;
            this.ipDatabaseProvider = ipDatabaseProvider;
        }

        @Override
        public Processor create(
            final Map<String, Processor.Factory> registry,
            final String processorTag,
            final String description,
            final Map<String, Object> config,
            final ProjectId projectId
        ) throws IOException {
            String ipField = readStringProperty(type, processorTag, config, "field");
            String targetField = readStringProperty(type, processorTag, config, "target_field", type);
            String databaseFile = readStringProperty(type, processorTag, config, "database_file", "GeoLite2-City.mmdb");
            List<String> propertyNames = readOptionalList(type, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(type, processorTag, config, "ignore_missing", false);
            boolean firstOnly = readBooleanProperty(type, processorTag, config, "first_only", true);

            // validate (and consume) the download_database_on_pipeline_creation property even though the result is not used by the factory
            readBooleanProperty(type, processorTag, config, "download_database_on_pipeline_creation", true);

            final String databaseType;
            try (IpDatabase ipDatabase = ipDatabaseProvider.getDatabase(projectId, databaseFile)) {
                if (ipDatabase == null) {
                    // It's possible that the database could be downloaded via the GeoipDownloader process and could become available
                    // at a later moment, so a processor impl is returned that tags documents instead. If a database cannot be sourced
                    // then the processor will continue to tag documents with a warning until it is remediated by providing a database
                    // or changing the pipeline.
                    return new DatabaseUnavailableProcessor(type, processorTag, description, databaseFile);
                }
                databaseType = ipDatabase.getDatabaseType();
            }

            final IpDataLookupFactory factory;
            try {
                factory = IpDataLookupFactories.get(databaseType, databaseFile);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(type, processorTag, "database_file", e.getMessage());
            }

            // the "geoip" processor type does additional validation of the database_type
            if (GEOIP_TYPE.equals(type)) {
                // type sniffing is done with the lowercased type
                final String lowerCaseDatabaseType = databaseType.toLowerCase(Locale.ROOT);

                // start with a strict positive rejection check -- as we support addition database providers,
                // we should expand these checks when possible
                if (lowerCaseDatabaseType.startsWith(IpinfoIpDataLookups.IPINFO_PREFIX)) {
                    throw newConfigurationException(
                        type,
                        processorTag,
                        "database_file",
                        Strings.format("Unsupported database type [%s] for file [%s]", databaseType, databaseFile)
                    );
                }

                // end with a lax negative rejection check -- if we aren't *certain* it's a maxmind database, then we'll warn --
                // it's possible for example that somebody cooked up a custom database of their own that happened to work with
                // our preexisting code, they should migrate to the new processor, but we're not going to break them right now
                if (lowerCaseDatabaseType.startsWith(MaxmindIpDataLookups.GEOIP2_PREFIX) == false
                    && lowerCaseDatabaseType.startsWith(MaxmindIpDataLookups.GEOLITE2_PREFIX) == false) {
                    deprecationLogger.warn(
                        DeprecationCategory.OTHER,
                        "unsupported_database_type",
                        UNSUPPORTED_DATABASE_DEPRECATION_MESSAGE,
                        databaseType
                    );
                }
            }

            final IpDataLookup ipDataLookup;
            try {
                ipDataLookup = factory.create(propertyNames);
            } catch (IllegalArgumentException e) {
                throw newConfigurationException(type, processorTag, "properties", e.getMessage());
            }

            return new GeoIpProcessor(
                type,
                processorTag,
                description,
                ipField,
                new DatabaseVerifyingSupplier(ipDatabaseProvider, databaseFile, databaseType, projectId),
                () -> ipDatabaseProvider.isValid(projectId, databaseFile),
                targetField,
                ipDataLookup,
                ignoreMissing,
                firstOnly,
                databaseFile
            );
        }

        /**
         * Get the value of the "download_database_on_pipeline_creation" property from a processor's config map.
         * <p>
         * As with the actual property definition, the default value of the property is 'true'. Unlike the actual
         * property definition, this method doesn't consume (that is, <code>config.remove</code>) the property from
         * the config map.
         */
        public static boolean downloadDatabaseOnPipelineCreation(Map<String, Object> config) {
            return (boolean) config.getOrDefault("download_database_on_pipeline_creation", true);
        }
    }

    static class DatabaseUnavailableProcessor extends AbstractProcessor {

        private final String type;
        private final String databaseName;

        DatabaseUnavailableProcessor(String type, String tag, String description, String databaseName) {
            super(tag, description);
            this.type = type;
            this.databaseName = databaseName;
        }

        @Override
        public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
            tag(ingestDocument, this.type, databaseName);
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

    private static void tag(IngestDocument ingestDocument, String type, String databaseName) {
        ingestDocument.appendFieldValue("tags", "_" + type + "_database_unavailable_" + databaseName, true);
    }
}
