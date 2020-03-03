/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.City;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Country;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Subdivision;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin.GeoIpCache;

import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;
import static org.elasticsearch.ingest.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.ConfigurationUtils.readStringProperty;

public final class GeoIpProcessor extends AbstractProcessor {

    public static final String TYPE = "geoip";
    private static final String CITY_DB_SUFFIX = "-City";
    private static final String COUNTRY_DB_SUFFIX = "-Country";
    private static final String ASN_DB_SUFFIX = "-ASN";

    private final String field;
    private final String targetField;
    private final DatabaseReaderLazyLoader lazyLoader;
    private final Set<Property> properties;
    private final boolean ignoreMissing;
    private final GeoIpCache cache;
    private final boolean firstOnly;

    /**
     * Construct a geo-IP processor.
     *
     * @param tag           the processor tag
     * @param field         the source field to geo-IP map
     * @param lazyLoader    a supplier of a geo-IP database reader; ideally this is lazily-loaded once on first use
     * @param targetField   the target field
     * @param properties    the properties; ideally this is lazily-loaded once on first use
     * @param ignoreMissing true if documents with a missing value for the field should be ignored
     * @param cache         a geo-IP cache
     * @param firstOnly     true if only first result should be returned in case of array
     */
    GeoIpProcessor(
        final String tag,
        final String field,
        final DatabaseReaderLazyLoader lazyLoader,
        final String targetField,
        final Set<Property> properties,
        final boolean ignoreMissing,
        final GeoIpCache cache,
        boolean firstOnly) {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.lazyLoader = lazyLoader;
        this.properties = properties;
        this.ignoreMissing = ignoreMissing;
        this.cache = cache;
        this.firstOnly = firstOnly;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws IOException {
        Object ip = ingestDocument.getFieldValue(field, Object.class, ignoreMissing);

        if (ip == null && ignoreMissing) {
            return ingestDocument;
        } else if (ip == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract geoip information.");
        }

        if (ip instanceof String) {
            Map<String, Object> geoData = getGeoData((String) ip);
            if (geoData.isEmpty() == false) {
                ingestDocument.setFieldValue(targetField, geoData);
            }
        } else if (ip instanceof List) {
            boolean match = false;
            List<Map<String, Object>> geoDataList = new ArrayList<>(((List) ip).size());
            for (Object ipAddr : (List) ip) {
                if (ipAddr instanceof String == false) {
                    throw new IllegalArgumentException("array in field [" + field + "] should only contain strings");
                }
                Map<String, Object> geoData = getGeoData((String) ipAddr);
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
        return ingestDocument;
    }

    private Map<String, Object> getGeoData(String ip) throws IOException {
        String databaseType = lazyLoader.getDatabaseType();
        final InetAddress ipAddress = InetAddresses.forString(ip);
        Map<String, Object> geoData;
        if (databaseType.endsWith(CITY_DB_SUFFIX)) {
            try {
                geoData = retrieveCityGeoData(ipAddress);
            } catch (AddressNotFoundRuntimeException e) {
                geoData = Collections.emptyMap();
            }
        } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
            try {
                geoData = retrieveCountryGeoData(ipAddress);
            } catch (AddressNotFoundRuntimeException e) {
                geoData = Collections.emptyMap();
            }
        } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
            try {
                geoData = retrieveAsnGeoData(ipAddress);
            } catch (AddressNotFoundRuntimeException e) {
                geoData = Collections.emptyMap();
            }
        } else {
            throw new ElasticsearchParseException("Unsupported database type [" + lazyLoader.getDatabaseType()
                + "]", new IllegalStateException());
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
        return lazyLoader.getDatabaseType();
    }

    Set<Property> getProperties() {
        return properties;
    }

    private Map<String, Object> retrieveCityGeoData(InetAddress ipAddress) {
        SpecialPermission.check();
        CityResponse response = AccessController.doPrivileged((PrivilegedAction<CityResponse>) () ->
            cache.putIfAbsent(ipAddress, CityResponse.class, ip -> {
                try {
                    return lazyLoader.get().city(ip);
                } catch (AddressNotFoundException e) {
                    throw new AddressNotFoundRuntimeException(e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));

        Country country = response.getCountry();
        City city = response.getCity();
        Location location = response.getLocation();
        Continent continent = response.getContinent();
        Subdivision subdivision = response.getMostSpecificSubdivision();

        Map<String, Object> geoData = new HashMap<>();
        for (Property property : this.properties) {
            switch (property) {
                case IP:
                    geoData.put("ip", NetworkAddress.format(ipAddress));
                    break;
                case COUNTRY_ISO_CODE:
                    String countryIsoCode = country.getIsoCode();
                    if (countryIsoCode != null) {
                        geoData.put("country_iso_code", countryIsoCode);
                    }
                    break;
                case COUNTRY_NAME:
                    String countryName = country.getName();
                    if (countryName != null) {
                        geoData.put("country_name", countryName);
                    }
                    break;
                case CONTINENT_NAME:
                    String continentName = continent.getName();
                    if (continentName != null) {
                        geoData.put("continent_name", continentName);
                    }
                    break;
                case REGION_ISO_CODE:
                    // ISO 3166-2 code for country subdivisions.
                    // See iso.org/iso-3166-country-codes.html
                    String countryIso = country.getIsoCode();
                    String subdivisionIso = subdivision.getIsoCode();
                    if (countryIso != null && subdivisionIso != null) {
                        String regionIsoCode = countryIso + "-" + subdivisionIso;
                        geoData.put("region_iso_code", regionIsoCode);
                    }
                    break;
                case REGION_NAME:
                    String subdivisionName = subdivision.getName();
                    if (subdivisionName != null) {
                        geoData.put("region_name", subdivisionName);
                    }
                    break;
                case CITY_NAME:
                    String cityName = city.getName();
                    if (cityName != null) {
                        geoData.put("city_name", cityName);
                    }
                    break;
                case TIMEZONE:
                    String locationTimeZone = location.getTimeZone();
                    if (locationTimeZone != null) {
                        geoData.put("timezone", locationTimeZone);
                    }
                    break;
                case LOCATION:
                    Double latitude = location.getLatitude();
                    Double longitude = location.getLongitude();
                    if (latitude != null && longitude != null) {
                        Map<String, Object> locationObject = new HashMap<>();
                        locationObject.put("lat", latitude);
                        locationObject.put("lon", longitude);
                        geoData.put("location", locationObject);
                    }
                    break;
            }
        }
        return geoData;
    }

    private Map<String, Object> retrieveCountryGeoData(InetAddress ipAddress) {
        SpecialPermission.check();
        CountryResponse response = AccessController.doPrivileged((PrivilegedAction<CountryResponse>) () ->
            cache.putIfAbsent(ipAddress, CountryResponse.class, ip -> {
                try {
                    return lazyLoader.get().country(ip);
                } catch (AddressNotFoundException e) {
                    throw new AddressNotFoundRuntimeException(e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));

        Country country = response.getCountry();
        Continent continent = response.getContinent();

        Map<String, Object> geoData = new HashMap<>();
        for (Property property : this.properties) {
            switch (property) {
                case IP:
                    geoData.put("ip", NetworkAddress.format(ipAddress));
                    break;
                case COUNTRY_ISO_CODE:
                    String countryIsoCode = country.getIsoCode();
                    if (countryIsoCode != null) {
                        geoData.put("country_iso_code", countryIsoCode);
                    }
                    break;
                case COUNTRY_NAME:
                    String countryName = country.getName();
                    if (countryName != null) {
                        geoData.put("country_name", countryName);
                    }
                    break;
                case CONTINENT_NAME:
                    String continentName = continent.getName();
                    if (continentName != null) {
                        geoData.put("continent_name", continentName);
                    }
                    break;
            }
        }
        return geoData;
    }

    private Map<String, Object> retrieveAsnGeoData(InetAddress ipAddress) {
        SpecialPermission.check();
        AsnResponse response = AccessController.doPrivileged((PrivilegedAction<AsnResponse>) () ->
            cache.putIfAbsent(ipAddress, AsnResponse.class, ip -> {
                try {
                    return lazyLoader.get().asn(ip);
                } catch (AddressNotFoundException e) {
                    throw new AddressNotFoundRuntimeException(e);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));

        Integer asn = response.getAutonomousSystemNumber();
        String organization_name = response.getAutonomousSystemOrganization();

        Map<String, Object> geoData = new HashMap<>();
        for (Property property : this.properties) {
            switch (property) {
                case IP:
                    geoData.put("ip", NetworkAddress.format(ipAddress));
                    break;
                case ASN:
                    if (asn != null) {
                        geoData.put("asn", asn);
                    }
                    break;
                case ORGANIZATION_NAME:
                    if (organization_name != null) {
                        geoData.put("organization_name", organization_name);
                    }
                    break;
            }
        }
        return geoData;
    }

    public static final class Factory implements Processor.Factory {
        static final Set<Property> DEFAULT_CITY_PROPERTIES = Collections.unmodifiableSet(EnumSet.of(
            Property.CONTINENT_NAME, Property.COUNTRY_ISO_CODE, Property.REGION_ISO_CODE,
            Property.REGION_NAME, Property.CITY_NAME, Property.LOCATION
        ));
        static final Set<Property> DEFAULT_COUNTRY_PROPERTIES = Collections.unmodifiableSet(EnumSet.of(
            Property.CONTINENT_NAME, Property.COUNTRY_ISO_CODE
        ));
        static final Set<Property> DEFAULT_ASN_PROPERTIES = Collections.unmodifiableSet(EnumSet.of(
            Property.IP, Property.ASN, Property.ORGANIZATION_NAME
        ));

        private final Map<String, DatabaseReaderLazyLoader> databaseReaders;

        Map<String, DatabaseReaderLazyLoader> databaseReaders() {
            return Collections.unmodifiableMap(databaseReaders);
        }

        private final GeoIpCache cache;

        public Factory(Map<String, DatabaseReaderLazyLoader> databaseReaders, GeoIpCache cache) {
            this.databaseReaders = databaseReaders;
            this.cache = cache;
        }

        @Override
        public GeoIpProcessor create(
            final Map<String, Processor.Factory> registry,
            final String processorTag,
            final Map<String, Object> config) throws IOException {
            String ipField = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "geoip");
            String databaseFile = readStringProperty(TYPE, processorTag, config, "database_file", "GeoLite2-City.mmdb");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);
            boolean firstOnly = readBooleanProperty(TYPE, processorTag, config, "first_only", true);

            DatabaseReaderLazyLoader lazyLoader = databaseReaders.get(databaseFile);
            if (lazyLoader == null) {
                throw newConfigurationException(TYPE, processorTag,
                    "database_file", "database file [" + databaseFile + "] doesn't exist");
            }

            final String databaseType = lazyLoader.getDatabaseType();

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
                properties = Collections.unmodifiableSet(modifiableProperties);
            } else {
                if (databaseType.endsWith(CITY_DB_SUFFIX)) {
                    properties = DEFAULT_CITY_PROPERTIES;
                } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
                    properties = DEFAULT_COUNTRY_PROPERTIES;
                } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
                    properties = DEFAULT_ASN_PROPERTIES;
                } else {
                    throw newConfigurationException(TYPE, processorTag, "database_file", "Unsupported database type ["
                        + databaseType + "]");
                }
            }

            return new GeoIpProcessor(processorTag, ipField, lazyLoader, targetField, properties, ignoreMissing, cache, firstOnly);
        }
    }

    // Geoip2's AddressNotFoundException is checked and due to the fact that we need run their code
    // inside a PrivilegedAction code block, we are forced to catch any checked exception and rethrow
    // it with an unchecked exception.
    //package private for testing
    static final class AddressNotFoundRuntimeException extends RuntimeException {

        AddressNotFoundRuntimeException(Throwable cause) {
            super(cause);
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
        ORGANIZATION_NAME;

        static final EnumSet<Property> ALL_CITY_PROPERTIES = EnumSet.of(
            Property.IP, Property.COUNTRY_ISO_CODE, Property.COUNTRY_NAME, Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE, Property.REGION_NAME, Property.CITY_NAME, Property.TIMEZONE,
            Property.LOCATION
        );
        static final EnumSet<Property> ALL_COUNTRY_PROPERTIES = EnumSet.of(
            Property.IP, Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE
        );
        static final EnumSet<Property> ALL_ASN_PROPERTIES = EnumSet.of(
            Property.IP, Property.ASN, Property.ORGANIZATION_NAME
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
                throw new IllegalArgumentException("illegal property value [" + value + "]. valid values are " +
                    Arrays.toString(validProperties.toArray()));
            }
        }
    }
}
