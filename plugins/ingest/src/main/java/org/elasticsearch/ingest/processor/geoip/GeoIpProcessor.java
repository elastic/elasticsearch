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
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.*;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;

import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

public final class GeoIpProcessor implements Processor {

    public static final String TYPE = "geoip";

    private final String ipField;
    private final String targetField;
    // pck-protected visibility for tests:
    final DatabaseReader dbReader;

    GeoIpProcessor(String ipField, DatabaseReader dbReader, String targetField) throws IOException {
        this.ipField = ipField;
        this.targetField = targetField == null ? "geoip" : targetField;
        this.dbReader = dbReader;
    }

    @Override
    public void execute(Data data) {
        String ip = data.getProperty(ipField);
        final InetAddress ipAddress;
        try {
            ipAddress = InetAddress.getByName(ip);
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        final Map<String, Object> geoData;
        switch (dbReader.getMetadata().getDatabaseType()) {
            case "GeoLite2-City":
                geoData = retrieveCityGeoData(ipAddress);
                break;
            case "GeoLite2-Country":
                geoData = retrieveCountryGeoData(ipAddress);
                break;
            default:
                throw new IllegalStateException("Unsupported database type [" + dbReader.getMetadata().getDatabaseType() + "]");
        }
        data.addField(targetField, geoData);
    }

    private Map<String, Object> retrieveCityGeoData(InetAddress ipAddress) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        CityResponse response = AccessController.doPrivileged(new PrivilegedAction<CityResponse>() {
            @Override
            public CityResponse run() {
                try {
                    return dbReader.city(ipAddress);
                } catch (IOException | GeoIp2Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Country country = response.getCountry();
        City city = response.getCity();
        Location location = response.getLocation();
        Continent continent = response.getContinent();
        Subdivision subdivision = response.getMostSpecificSubdivision();

        Map<String, Object> geoData = new HashMap<String, Object>();
        geoData.put("ip", NetworkAddress.formatAddress(ipAddress));
        geoData.put("country_iso_code", country.getIsoCode());
        geoData.put("country_name", country.getName());
        geoData.put("continent_name", continent.getName());
        geoData.put("region_name", subdivision.getName());
        geoData.put("city_name", city.getName());
        geoData.put("timezone", location.getTimeZone());
        geoData.put("latitude", location.getLatitude());
        geoData.put("longitude", location.getLongitude());
        if (location.getLatitude() != null && location.getLongitude() != null) {
            geoData.put("location", new double[]{location.getLongitude(), location.getLatitude()});
        }
        return geoData;
    }

    private Map<String, Object> retrieveCountryGeoData(InetAddress ipAddress) {
        SecurityManager sm = System.getSecurityManager();
        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        }
        CountryResponse response = AccessController.doPrivileged(new PrivilegedAction<CountryResponse>() {
            @Override
            public CountryResponse run() {
                try {
                    return dbReader.country(ipAddress);
                } catch (IOException | GeoIp2Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Country country = response.getCountry();
        Continent continent = response.getContinent();

        Map<String, Object> geoData = new HashMap<String, Object>();
        geoData.put("ip", NetworkAddress.formatAddress(ipAddress));
        geoData.put("country_iso_code", country.getIsoCode());
        geoData.put("country_name", country.getName());
        geoData.put("continent_name", continent.getName());
        return geoData;
    }

    public static class Builder implements Processor.Builder {

        private final Path geoIpConfigDirectory;
        private final DatabaseReaderService databaseReaderService;

        private String ipField;
        private String databaseFile = "GeoLite2-City.mmdb";
        private String targetField = "geoip";

        public Builder(Path geoIpConfigDirectory, DatabaseReaderService databaseReaderService) {
            this.geoIpConfigDirectory = geoIpConfigDirectory;
            this.databaseReaderService = databaseReaderService;
        }

        public void setIpField(String ipField) {
            this.ipField = ipField;
        }

        public void setDatabaseFile(String dbPath) {
            this.databaseFile = dbPath;
        }

        public void setTargetField(String targetField) {
            this.targetField = targetField;
        }

        public void fromMap(Map<String, Object> config) {
            this.ipField = (String) config.get("ip_field");

            String targetField = (String) config.get("target_field");
            if (targetField != null) {
                this.targetField = targetField;
            }
            String databaseFile = (String) config.get("database_file");
            if (databaseFile != null) {
                this.databaseFile = databaseFile;
            }
        }

        @Override
        public Processor build() throws IOException {
            Path databasePath = geoIpConfigDirectory.resolve(databaseFile);
            if (Files.exists(databasePath)) {
                try (InputStream database = Files.newInputStream(databasePath, StandardOpenOption.READ)) {
                    DatabaseReader databaseReader = databaseReaderService.getOrCreateDatabaseReader(databaseFile, database);
                    return new GeoIpProcessor(ipField, databaseReader, targetField);
                }
            } else {
                throw new IllegalArgumentException("database file [" + databaseFile + "] doesn't exist in [" + geoIpConfigDirectory + "]");
            }
        }

        public static class Factory implements Processor.Builder.Factory {

            private Path geoIpConfigDirectory;
            private final DatabaseReaderService databaseReaderService = new DatabaseReaderService();

            @Override
            public Processor.Builder create() {
                return new Builder(geoIpConfigDirectory, databaseReaderService);
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

    }

}
