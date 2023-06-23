/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.Network;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Subdivision;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
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

    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(GeoIpProcessor.class);
    static final String DEFAULT_DATABASES_DEPRECATION_MESSAGE = "the [fallback_to_default_databases] has been deprecated, because "
        + "Elasticsearch no longer includes the default Maxmind geoip databases. This setting will be removed in Elasticsearch 9.0";

    public static final String TYPE = "geoip";
    private static final String CITY_DB_SUFFIX = "-City";
    private static final String COUNTRY_DB_SUFFIX = "-Country";
    private static final String ASN_DB_SUFFIX = "-ASN";

    private final String field;
    private final Supplier<Boolean> isValid;
    private final String targetField;
    private final CheckedSupplier<GeoIpDatabase, IOException> supplier;
    private final Set<Property> properties;
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
     * @param properties    the properties; ideally this is lazily-loaded once on first use
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
        final Set<Property> properties,
        final boolean ignoreMissing,
        final boolean firstOnly,
        final String databaseFile
    ) {
        super(tag, description);
        this.field = field;
        this.isValid = isValid;
        this.targetField = targetField;
        this.supplier = supplier;
        this.properties = properties;
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
        final String databaseType = geoIpDatabase.getDatabaseType();
        final InetAddress ipAddress = InetAddresses.forString(ip);
        Map<String, Object> geoData;
        if (databaseType.endsWith(CITY_DB_SUFFIX)) {
            geoData = retrieveCityGeoData(geoIpDatabase, ipAddress);
        } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
            geoData = retrieveCountryGeoData(geoIpDatabase, ipAddress);

        } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
            geoData = retrieveAsnGeoData(geoIpDatabase, ipAddress);

        } else {
            throw new ElasticsearchParseException(
                "Unsupported database type [" + geoIpDatabase.getDatabaseType() + "]",
                new IllegalStateException()
            );
        }
        return geoData;
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
        return properties;
    }

    private Map<String, Object> retrieveCityGeoData(GeoIpDatabase geoIpDatabase, InetAddress ipAddress) {
        CityResponse response = geoIpDatabase.getCity(ipAddress);
        if (response == null) {
            return Map.of();
        }
        Country country = response.getCountry();
        City city = response.getCity();
        Location location = response.getLocation();
        Continent continent = response.getContinent();
        Subdivision subdivision = response.getMostSpecificSubdivision();

        Map<String, Object> geoData = new HashMap<>();
        for (Property property : this.properties) {
            switch (property) {
                case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                case COUNTRY_ISO_CODE -> {
                    String countryIsoCode = country.getIsoCode();
                    if (countryIsoCode != null) {
                        geoData.put("country_iso_code", countryIsoCode);
                    }
                }
                case COUNTRY_NAME -> {
                    String countryName = country.getName();
                    if (countryName != null) {
                        geoData.put("country_name", countryName);
                    }
                }
                case CONTINENT_NAME -> {
                    String continentName = continent.getName();
                    if (continentName != null) {
                        geoData.put("continent_name", continentName);
                    }
                }
                case REGION_ISO_CODE -> {
                    // ISO 3166-2 code for country subdivisions.
                    // See iso.org/iso-3166-country-codes.html
                    String countryIso = country.getIsoCode();
                    String subdivisionIso = subdivision.getIsoCode();
                    if (countryIso != null && subdivisionIso != null) {
                        String regionIsoCode = countryIso + "-" + subdivisionIso;
                        geoData.put("region_iso_code", regionIsoCode);
                    }
                }
                case REGION_NAME -> {
                    String subdivisionName = subdivision.getName();
                    if (subdivisionName != null) {
                        geoData.put("region_name", subdivisionName);
                    }
                }
                case CITY_NAME -> {
                    String cityName = city.getName();
                    if (cityName != null) {
                        geoData.put("city_name", cityName);
                    }
                }
                case TIMEZONE -> {
                    String locationTimeZone = location.getTimeZone();
                    if (locationTimeZone != null) {
                        geoData.put("timezone", locationTimeZone);
                    }
                }
                case LOCATION -> {
                    Double latitude = location.getLatitude();
                    Double longitude = location.getLongitude();
                    if (latitude != null && longitude != null) {
                        Map<String, Object> locationObject = new HashMap<>();
                        locationObject.put("lat", latitude);
                        locationObject.put("lon", longitude);
                        geoData.put("location", locationObject);
                    }
                }
            }
        }
        return geoData;
    }

    private Map<String, Object> retrieveCountryGeoData(GeoIpDatabase geoIpDatabase, InetAddress ipAddress) {
        CountryResponse response = geoIpDatabase.getCountry(ipAddress);
        if (response == null) {
            return Map.of();
        }
        Country country = response.getCountry();
        Continent continent = response.getContinent();

        Map<String, Object> geoData = new HashMap<>();
        for (Property property : this.properties) {
            switch (property) {
                case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                case COUNTRY_ISO_CODE -> {
                    String countryIsoCode = country.getIsoCode();
                    if (countryIsoCode != null) {
                        geoData.put("country_iso_code", countryIsoCode);
                    }
                }
                case COUNTRY_NAME -> {
                    String countryName = country.getName();
                    if (countryName != null) {
                        geoData.put("country_name", countryName);
                    }
                }
                case CONTINENT_NAME -> {
                    String continentName = continent.getName();
                    if (continentName != null) {
                        geoData.put("continent_name", continentName);
                    }
                }
            }
        }
        return geoData;
    }

    private Map<String, Object> retrieveAsnGeoData(GeoIpDatabase geoIpDatabase, InetAddress ipAddress) {
        AsnResponse response = geoIpDatabase.getAsn(ipAddress);
        if (response == null) {
            return Map.of();
        }
        Long asn = response.getAutonomousSystemNumber();
        String organization_name = response.getAutonomousSystemOrganization();
        Network network = response.getNetwork();

        Map<String, Object> geoData = new HashMap<>();
        for (Property property : this.properties) {
            switch (property) {
                case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                case ASN -> {
                    if (asn != null) {
                        geoData.put("asn", asn);
                    }
                }
                case ORGANIZATION_NAME -> {
                    if (organization_name != null) {
                        geoData.put("organization_name", organization_name);
                    }
                }
                case NETWORK -> {
                    if (network != null) {
                        geoData.put("network", network.toString());
                    }
                }
            }
        }
        return geoData;
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
            if (Factory.useDatabaseUnavailableProcessor(loader, databaseFile)) {
                return null;
            } else if (loader == null) {
                throw new ResourceNotFoundException("database file [" + databaseFile + "] doesn't exist");
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
        static final Set<Property> DEFAULT_CITY_PROPERTIES = Set.copyOf(
            EnumSet.of(
                Property.CONTINENT_NAME,
                Property.COUNTRY_NAME,
                Property.COUNTRY_ISO_CODE,
                Property.REGION_ISO_CODE,
                Property.REGION_NAME,
                Property.CITY_NAME,
                Property.LOCATION
            )
        );
        static final Set<Property> DEFAULT_COUNTRY_PROPERTIES = Set.copyOf(
            EnumSet.of(Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE)
        );
        static final Set<Property> DEFAULT_ASN_PROPERTIES = Set.copyOf(
            EnumSet.of(Property.IP, Property.ASN, Property.ORGANIZATION_NAME, Property.NETWORK)
        );

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
                DEPRECATION_LOGGER.warn(DeprecationCategory.OTHER, "default_databases_message", DEFAULT_DATABASES_DEPRECATION_MESSAGE);
            }

            GeoIpDatabase geoIpDatabase = geoIpDatabaseProvider.getDatabase(databaseFile);
            if (useDatabaseUnavailableProcessor(geoIpDatabase, databaseFile)) {
                return new DatabaseUnavailableProcessor(processorTag, description, databaseFile);
            } else if (geoIpDatabase == null) {
                throw newConfigurationException(TYPE, processorTag, "database_file", "database file [" + databaseFile + "] doesn't exist");
            }
            final String databaseType;
            try {
                databaseType = geoIpDatabase.getDatabaseType();
            } finally {
                geoIpDatabase.release();
            }

            final Set<Property> properties;
            if (propertyNames != null) {
                Set<Property> modifiableProperties = EnumSet.noneOf(Property.class);
                for (String fieldName : propertyNames) {
                    try {
                        modifiableProperties.add(Property.parseProperty(databaseType, fieldName));
                    } catch (IllegalArgumentException e) {
                        throw newConfigurationException(TYPE, processorTag, "properties", e.getMessage());
                    }
                }
                properties = Set.copyOf(modifiableProperties);
            } else {
                if (databaseType.endsWith(CITY_DB_SUFFIX)) {
                    properties = DEFAULT_CITY_PROPERTIES;
                } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
                    properties = DEFAULT_COUNTRY_PROPERTIES;
                } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
                    properties = DEFAULT_ASN_PROPERTIES;
                } else {
                    throw newConfigurationException(
                        TYPE,
                        processorTag,
                        "database_file",
                        "Unsupported database type [" + databaseType + "]"
                    );
                }
            }
            return new GeoIpProcessor(
                processorTag,
                description,
                ipField,
                new DatabaseVerifyingSupplier(geoIpDatabaseProvider, databaseFile, databaseType),
                () -> geoIpDatabaseProvider.isValid(databaseFile),
                targetField,
                properties,
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

        private static boolean useDatabaseUnavailableProcessor(GeoIpDatabase database, String databaseName) {
            // If there is no instance for a database we should fail with a config error, but
            // if there is no instance for a builtin database that we manage via GeoipDownloader then don't fail.
            // In the latter case the database should become available at a later moment, so a processor impl
            // is returned that tags documents instead.
            return database == null && IngestGeoIpPlugin.DEFAULT_DATABASE_FILENAMES.contains(databaseName);
        }

    }

    enum Property {

        IP,
        COUNTRY_ISO_CODE,
        COUNTRY_NAME,
        CONTINENT_NAME,
        REGION_ISO_CODE,
        REGION_NAME,
        CITY_NAME,
        TIMEZONE,
        LOCATION,
        ASN,
        ORGANIZATION_NAME,
        NETWORK;

        static final EnumSet<Property> ALL_CITY_PROPERTIES = EnumSet.of(
            Property.IP,
            Property.COUNTRY_ISO_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.TIMEZONE,
            Property.LOCATION
        );
        static final EnumSet<Property> ALL_COUNTRY_PROPERTIES = EnumSet.of(
            Property.IP,
            Property.CONTINENT_NAME,
            Property.COUNTRY_NAME,
            Property.COUNTRY_ISO_CODE
        );
        static final EnumSet<Property> ALL_ASN_PROPERTIES = EnumSet.of(
            Property.IP,
            Property.ASN,
            Property.ORGANIZATION_NAME,
            Property.NETWORK
        );

        public static Property parseProperty(String databaseType, String value) {
            Set<Property> validProperties = EnumSet.noneOf(Property.class);
            if (databaseType.endsWith(CITY_DB_SUFFIX)) {
                validProperties = ALL_CITY_PROPERTIES;
            } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
                validProperties = ALL_COUNTRY_PROPERTIES;
            } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
                validProperties = ALL_ASN_PROPERTIES;
            }

            try {
                Property property = valueOf(value.toUpperCase(Locale.ROOT));
                if (validProperties.contains(property) == false) {
                    throw new IllegalArgumentException("invalid");
                }
                return property;
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "illegal property value [" + value + "]. valid values are " + Arrays.toString(validProperties.toArray())
                );
            }
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
