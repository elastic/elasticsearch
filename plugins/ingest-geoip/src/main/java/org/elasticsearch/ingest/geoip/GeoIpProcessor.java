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
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.ingest.core.AbstractProcessorFactory;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;

import java.io.Closeable;
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

import static org.elasticsearch.ingest.core.ConfigurationUtils.readOptionalList;
import static org.elasticsearch.ingest.core.ConfigurationUtils.readStringProperty;

public final class GeoIpProcessor implements Processor {

    public static final String TYPE = "geoip";

    private final String processorTag;
    private final String sourceField;
    private final String targetField;
    private final DatabaseReader dbReader;
    private final Set<Field> fields;

    GeoIpProcessor(String processorTag, String sourceField, DatabaseReader dbReader, String targetField, Set<Field> fields) throws IOException {
        this.processorTag = processorTag;
        this.sourceField = sourceField;
        this.targetField = targetField;
        this.dbReader = dbReader;
        this.fields = fields;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        String ip = ingestDocument.getFieldValue(sourceField, String.class);
        final InetAddress ipAddress = InetAddresses.forString(ip);

        Map<String, Object> geoData;
        switch (dbReader.getMetadata().getDatabaseType()) {
            case "GeoLite2-City":
                try {
                    geoData = retrieveCityGeoData(ipAddress);
                } catch (AddressNotFoundRuntimeException e) {
                    geoData = Collections.emptyMap();
                }
                break;
            case "GeoLite2-Country":
                try {
                    geoData = retrieveCountryGeoData(ipAddress);
                } catch (AddressNotFoundRuntimeException e) {
                    geoData = Collections.emptyMap();
                }
                break;
            default:
                throw new IllegalStateException("Unsupported database type [" + dbReader.getMetadata().getDatabaseType() + "]");
        }
        ingestDocument.setFieldValue(targetField, geoData);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public String getTag() {
        return processorTag;
    }

    String getSourceField() {
        return sourceField;
    }

    String getTargetField() {
        return targetField;
    }

    DatabaseReader getDbReader() {
        return dbReader;
    }

    Set<Field> getFields() {
        return fields;
    }

    private Map<String, Object> retrieveCityGeoData(InetAddress ipAddress) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
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
        for (Field field : fields) {
            switch (field) {
                case IP:
                    geoData.put("ip", NetworkAddress.formatAddress(ipAddress));
                    break;
                case COUNTRY_ISO_CODE:
                    geoData.put("country_iso_code", country.getIsoCode());
                    break;
                case COUNTRY_NAME:
                    geoData.put("country_name", country.getName());
                    break;
                case CONTINENT_NAME:
                    geoData.put("continent_name", continent.getName());
                    break;
                case REGION_NAME:
                    geoData.put("region_name", subdivision.getName());
                    break;
                case CITY_NAME:
                    geoData.put("city_name", city.getName());
                    break;
                case TIMEZONE:
                    geoData.put("timezone", location.getTimeZone());
                    break;
                case LATITUDE:
                    geoData.put("latitude", location.getLatitude());
                    break;
                case LONGITUDE:
                    geoData.put("longitude", location.getLongitude());
                    break;
                case LOCATION:
                    if (location.getLatitude() != null && location.getLongitude() != null) {
                        geoData.put("location", Arrays.asList(location.getLongitude(), location.getLatitude()));
                    }
                    break;
            }
        }
        return geoData;
    }

    private Map<String, Object> retrieveCountryGeoData(InetAddress ipAddress) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
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
        for (Field field : fields) {
            switch (field) {
                case IP:
                    geoData.put("ip", NetworkAddress.formatAddress(ipAddress));
                    break;
                case COUNTRY_ISO_CODE:
                    geoData.put("country_iso_code", country.getIsoCode());
                    break;
                case COUNTRY_NAME:
                    geoData.put("country_name", country.getName());
                    break;
                case CONTINENT_NAME:
                    geoData.put("continent_name", continent.getName());
                    break;
            }
        }
        return geoData;
    }

    public static final class Factory extends AbstractProcessorFactory<GeoIpProcessor> implements Closeable {

        static final Set<Field> DEFAULT_FIELDS = EnumSet.of(
                Field.CONTINENT_NAME, Field.COUNTRY_ISO_CODE, Field.REGION_NAME, Field.CITY_NAME, Field.LOCATION
        );

        private final Map<String, DatabaseReader> databaseReaders;

        public Factory(Map<String, DatabaseReader> databaseReaders) {
            this.databaseReaders = databaseReaders;
        }

        @Override
        public GeoIpProcessor doCreate(String processorTag, Map<String, Object> config) throws Exception {
            String ipField = readStringProperty(config, "source_field");
            String targetField = readStringProperty(config, "target_field", "geoip");
            String databaseFile = readStringProperty(config, "database_file", "GeoLite2-City.mmdb");
            List<String> fieldNames = readOptionalList(config, "fields");

            final Set<Field> fields;
            if (fieldNames != null) {
                fields = EnumSet.noneOf(Field.class);
                for (String fieldName : fieldNames) {
                    try {
                        fields.add(Field.parse(fieldName));
                    } catch (Exception e) {
                        throw new IllegalArgumentException("illegal field option [" + fieldName +"]. valid values are [" + Arrays.toString(Field.values()) +"]", e);
                    }
                }
            } else {
                fields = DEFAULT_FIELDS;
            }

            DatabaseReader databaseReader = databaseReaders.get(databaseFile);
            if (databaseReader == null) {
                throw new IllegalArgumentException("database file [" + databaseFile + "] doesn't exist");
            }
            return new GeoIpProcessor(processorTag, ipField, databaseReader, targetField, fields);
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(databaseReaders.values());
        }
    }

    // Geoip2's AddressNotFoundException is checked and due to the fact that we need run their code
    // inside a PrivilegedAction code block, we are forced to catch any checked exception and rethrow
    // it with an unchecked exception.
    private final static class AddressNotFoundRuntimeException extends RuntimeException {

        public AddressNotFoundRuntimeException(Throwable cause) {
            super(cause);
        }
    }

    public enum Field {

        IP,
        COUNTRY_ISO_CODE,
        COUNTRY_NAME,
        CONTINENT_NAME,
        REGION_NAME,
        CITY_NAME,
        TIMEZONE,
        LATITUDE,
        LONGITUDE,
        LOCATION;

        public static Field parse(String value) {
            return valueOf(value.toUpperCase(Locale.ROOT));
        }
    }

}
