/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.DatabaseRecord;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;

import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.ingest.geoip.MaxmindIpDataLookups.CacheableCityResponse;
import org.elasticsearch.ingest.geoip.MaxmindIpDataLookups.CacheableCountryResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public final class GeoIpTestUtils {

    private GeoIpTestUtils() {
        // utility class
    }

    public static final Set<String> DEFAULT_DATABASES = Set.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb");

    @SuppressForbidden(reason = "uses java.io.File")
    private static boolean isDirectory(final Path path) {
        return path.toFile().isDirectory();
    }

    static void copyDatabase(final String databaseName, final Path destination) {
        try (InputStream is = GeoIpTestUtils.class.getResourceAsStream("/" + databaseName)) {
            if (is == null) {
                throw new FileNotFoundException("Resource [" + databaseName + "] not found in classpath");
            }

            Files.copy(is, isDirectory(destination) ? destination.resolve(databaseName) : destination, REPLACE_EXISTING);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static void copyDefaultDatabases(final Path directory) {
        for (final String database : DEFAULT_DATABASES) {
            copyDatabase(database, directory);
        }
    }

    static void copyDefaultDatabases(final Path directory, ConfigDatabases configDatabases) {
        for (final String database : DEFAULT_DATABASES) {
            copyDatabase(database, directory);
            configDatabases.updateDatabase(directory.resolve(database), true);
        }
    }

    /**
     * A static city-specific responseProvider for use with {@link IpDatabase#getResponse(String, CheckedBiFunction)} in
     * tests.
     * <p>
     * Like this: {@code CacheableCityResponse city = loader.getResponse("some.ip.address", GeoIpTestUtils::getCity);}
     */
    public static CacheableCityResponse getCity(Reader reader, String ip) throws IOException {
        DatabaseRecord<CityResponse> record = reader.getRecord(InetAddresses.forString(ip), CityResponse.class);
        CityResponse data = record.getData();
        return data == null ? null : CacheableCityResponse.from(new CityResponse(data, ip, record.getNetwork(), List.of("en")));
    }

    /**
     * A static country-specific responseProvider for use with {@link IpDatabase#getResponse(String, CheckedBiFunction)} in
     * tests.
     * <p>
     * Like this: {@code CacheableCountryResponse country = loader.getResponse("some.ip.address", GeoIpTestUtils::getCountry);}
     */
    public static CacheableCountryResponse getCountry(Reader reader, String ip) throws IOException {
        DatabaseRecord<CountryResponse> record = reader.getRecord(InetAddresses.forString(ip), CountryResponse.class);
        CountryResponse data = record.getData();
        return data == null ? null : CacheableCountryResponse.from(new CountryResponse(data, ip, record.getNetwork(), List.of("en")));
    }
}
