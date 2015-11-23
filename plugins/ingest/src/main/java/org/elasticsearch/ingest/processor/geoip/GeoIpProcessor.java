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

package org.elasticsearch.ingest.processor.geoip;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.AddressNotFoundException;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.*;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.processor.Processor;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

import static org.elasticsearch.ingest.processor.ConfigurationUtils.readList;
import static org.elasticsearch.ingest.processor.ConfigurationUtils.readStringProperty;

public final class GeoIpProcessor implements Processor {

    public static final String TYPE = "geoip";

    private final String sourceField;
    private final String targetField;
    private final DatabaseReader dbReader;
    private final Set<Field> fields;

    GeoIpProcessor(String sourceField, DatabaseReader dbReader, String targetField, Set<Field> fields) throws IOException {
        this.sourceField = sourceField;
        this.targetField = targetField;
        this.dbReader = dbReader;
        this.fields = fields;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        String ip = ingestDocument.getPropertyValue(sourceField, String.class);
        final InetAddress ipAddress;
        try {
            ipAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

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
        ingestDocument.setPropertyValue(targetField, geoData);
    }

    @Override
    public String getType() {
        return TYPE;
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
                        geoData.put("location", new double[]{location.getLongitude(), location.getLatitude()});
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

    public static class Factory implements Processor.Factory<GeoIpProcessor> {

        static final Set<Field> DEFAULT_FIELDS = EnumSet.of(
                Field.CONTINENT_NAME, Field.COUNTRY_ISO_CODE, Field.REGION_NAME, Field.CITY_NAME, Field.LOCATION
        );

        private Path geoIpConfigDirectory;
        private final DatabaseReaderService databaseReaderService = new DatabaseReaderService();

        public GeoIpProcessor create(Map<String, Object> config) throws IOException {
            String ipField = readStringProperty(config, "source_field");
            String targetField = readStringProperty(config, "target_field", "geoip");
            String databaseFile = readStringProperty(config, "database_file", "GeoLite2-City.mmdb");
            final Set<Field> fields;
            if (config.containsKey("fields")) {
                fields = EnumSet.noneOf(Field.class);
                List<String> fieldNames = readList(config, "fields");
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

            Path databasePath = geoIpConfigDirectory.resolve(databaseFile);
            if (Files.exists(databasePath) && Files.isRegularFile(databasePath)) {
                try (InputStream database = Files.newInputStream(databasePath, StandardOpenOption.READ)) {
                    DatabaseReader databaseReader = databaseReaderService.getOrCreateDatabaseReader(databaseFile, database);
                    return new GeoIpProcessor(ipField, databaseReader, targetField, fields);
                }
            } else {
                throw new IllegalArgumentException("database file [" + databaseFile + "] doesn't exist in [" + geoIpConfigDirectory + "]");
            }
        }

        @Override
        public void setConfigDirectory(Path configDirectory) {
            geoIpConfigDirectory = configDirectory.resolve("ingest").resolve("geoip");
        }

        @Override
        public void close() throws IOException {
            databaseReaderService.close();
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
