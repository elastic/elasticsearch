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

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
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

import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
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

    private final String field;
    private final String targetField;
    private final DatabaseReader dbReader;
    private final Set<Property> properties;
    private final boolean ignoreMissing;

    GeoIpProcessor(String tag, String field, DatabaseReader dbReader, String targetField, Set<Property> properties,
                   boolean ignoreMissing) throws IOException {
        super(tag);
        this.field = field;
        this.targetField = targetField;
        this.dbReader = dbReader;
        this.properties = properties;
        this.ignoreMissing = ignoreMissing;
    }

    boolean isIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        String ip = ingestDocument.getFieldValue(field, String.class, ignoreMissing);

        if (ip == null && ignoreMissing) {
            return;
        } else if (ip == null) {
            throw new IllegalArgumentException("field [" + field + "] is null, cannot extract geoip information.");
        }

        final InetAddress ipAddress = InetAddresses.forString(ip);

        Map<String, Object> geoData;
        String databaseType = dbReader.getMetadata().getDatabaseType();

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
        } else {
            throw new ElasticsearchParseException("Unsupported database type [" + dbReader.getMetadata().getDatabaseType()
                    + "]", new IllegalStateException());
        }
        if (geoData.isEmpty() == false) {
            ingestDocument.setFieldValue(targetField, geoData);
        }
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

    DatabaseReader getDbReader() {
        return dbReader;
    }

    Set<Property> getProperties() {
        return properties;
    }

    private Map<String, Object> retrieveCityGeoData(InetAddress ipAddress) {
        SpecialPermission.check();
        CityResponse response = AccessController.doPrivileged((PrivilegedAction<CityResponse>) () -> {
            try {
                return dbReader.city(ipAddress);
            } catch (AddressNotFoundException e) {
                throw new AddressNotFoundRuntimeException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

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
        CountryResponse response = AccessController.doPrivileged((PrivilegedAction<CountryResponse>) () -> {
            try {
                return dbReader.country(ipAddress);
            } catch (AddressNotFoundException e) {
                throw new AddressNotFoundRuntimeException(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

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

    public static final class Factory implements Processor.Factory {
        static final Set<Property> DEFAULT_CITY_PROPERTIES = EnumSet.of(
            Property.CONTINENT_NAME, Property.COUNTRY_ISO_CODE, Property.REGION_NAME,
            Property.CITY_NAME, Property.LOCATION
        );
        static final Set<Property> DEFAULT_COUNTRY_PROPERTIES = EnumSet.of(Property.CONTINENT_NAME, Property.COUNTRY_ISO_CODE);

        private final Map<String, DatabaseReaderLazyLoader> databaseReaders;

        public Factory(Map<String, DatabaseReaderLazyLoader> databaseReaders) {
            this.databaseReaders = databaseReaders;
        }

        @Override
        public GeoIpProcessor create(Map<String, Processor.Factory> registry, String processorTag,
                                       Map<String, Object> config) throws Exception {
            String ipField = readStringProperty(TYPE, processorTag, config, "field");
            String targetField = readStringProperty(TYPE, processorTag, config, "target_field", "geoip");
            String databaseFile = readStringProperty(TYPE, processorTag, config, "database_file", "GeoLite2-City.mmdb.gz");
            List<String> propertyNames = readOptionalList(TYPE, processorTag, config, "properties");
            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", false);

            DatabaseReaderLazyLoader lazyLoader = databaseReaders.get(databaseFile);
            if (lazyLoader == null) {
                throw newConfigurationException(TYPE, processorTag,
                    "database_file", "database file [" + databaseFile + "] doesn't exist");
            }

            DatabaseReader databaseReader = lazyLoader.get();
            String databaseType = databaseReader.getMetadata().getDatabaseType();

            final Set<Property> properties;
            if (propertyNames != null) {
                properties = EnumSet.noneOf(Property.class);
                for (String fieldName : propertyNames) {
                    try {
                        properties.add(Property.parseProperty(databaseType, fieldName));
                    } catch (IllegalArgumentException e) {
                        throw newConfigurationException(TYPE, processorTag, "properties", e.getMessage());
                    }
                }
            } else {
                if (databaseType.endsWith(CITY_DB_SUFFIX)) {
                    properties = DEFAULT_CITY_PROPERTIES;
                } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
                    properties = DEFAULT_COUNTRY_PROPERTIES;
                } else {
                    throw newConfigurationException(TYPE, processorTag, "database_file", "Unsupported database type ["
                            + databaseType + "]");
                }
            }

            return new GeoIpProcessor(processorTag, ipField, databaseReader, targetField, properties, ignoreMissing);
        }
    }

    // Geoip2's AddressNotFoundException is checked and due to the fact that we need run their code
    // inside a PrivilegedAction code block, we are forced to catch any checked exception and rethrow
    // it with an unchecked exception.
    private static final class AddressNotFoundRuntimeException extends RuntimeException {

        AddressNotFoundRuntimeException(Throwable cause) {
            super(cause);
        }
    }

    enum Property {

        IP,
        COUNTRY_ISO_CODE,
        COUNTRY_NAME,
        CONTINENT_NAME,
        REGION_NAME,
        CITY_NAME,
        TIMEZONE,
        LOCATION;

        static final EnumSet<Property> ALL_CITY_PROPERTIES = EnumSet.allOf(Property.class);
        static final EnumSet<Property> ALL_COUNTRY_PROPERTIES = EnumSet.of(Property.IP, Property.CONTINENT_NAME,
            Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE);

        public static Property parseProperty(String databaseType, String value) {
            Set<Property> validProperties = EnumSet.noneOf(Property.class);
            if (databaseType.endsWith(CITY_DB_SUFFIX)) {
                validProperties = ALL_CITY_PROPERTIES;
            } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
                validProperties = ALL_COUNTRY_PROPERTIES;
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
