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
import com.maxmind.db.MaxMindDbConstructor;
import com.maxmind.db.MaxMindDbParameter;
import com.maxmind.db.Reader;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A collection of {@link IpDataLookup} implementations for IPinfo databases
 */
final class IpinfoIpDataLookups {

    private IpinfoIpDataLookups() {
        // utility class
    }

    public record CountryResult(
        @MaxMindDbParameter(name = "continent") String continent,
        @MaxMindDbParameter(name = "continent_name") String continentName,
        @MaxMindDbParameter(name = "country") String country,
        @MaxMindDbParameter(name = "country_name") String countryName
    ) {
        @MaxMindDbConstructor
        public CountryResult {}
    }

    static class Country extends AbstractBase<CountryResult> {
        Country(Set<Database.Property> properties) {
            super(properties, CountryResult.class);
        }

        @Override
        protected Map<String, Object> transform(final Result<CountryResult> result) {
            CountryResult response = result.result;

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", result.ip);
                    case COUNTRY_ISO_CODE -> {
                        String countryIsoCode = response.country;
                        if (countryIsoCode != null) {
                            data.put("country_iso_code", countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        String countryName = response.countryName;
                        if (countryName != null) {
                            data.put("country_name", countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        String continentCode = response.continent;
                        if (continentCode != null) {
                            data.put("continent_code", continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        String continentName = response.continentName;
                        if (continentName != null) {
                            data.put("continent_name", continentName);
                        }
                    }
                }
            }
            return data;
        }
    }

    /**
     * Just a little record holder -- there's the data that we receive via the binding to our record objects from the Reader via the
     * getRecord call, but then we also need to capture the passed-in ip address that came from the caller as well as the network for
     * the returned DatabaseRecord from the Reader.
     */
    public record Result<T>(T result, String ip, String network) {}

    /**
     * The {@link IpinfoIpDataLookups.AbstractBase} is an abstract base implementation of {@link IpDataLookup} that
     * provides common functionality for getting a {@link IpinfoIpDataLookups.Result} that wraps a record from a {@link IpDatabase}.
     *
     * @param <RESPONSE> the record type that will be wrapped and returned
     */
    private abstract static class AbstractBase<RESPONSE> implements IpDataLookup {

        protected final Set<Database.Property> properties;
        protected final Class<RESPONSE> clazz;

        AbstractBase(final Set<Database.Property> properties, final Class<RESPONSE> clazz) {
            this.properties = Set.copyOf(properties);
            this.clazz = clazz;
        }

        @Override
        public Set<Database.Property> getProperties() {
            return this.properties;
        }

        @Override
        public final Map<String, Object> getData(final IpDatabase ipDatabase, final String ipAddress) {
            final Result<RESPONSE> response = ipDatabase.getResponse(ipAddress, this::lookup);
            return (response == null || response.result == null) ? Map.of() : transform(response);
        }

        @Nullable
        private Result<RESPONSE> lookup(final Reader reader, final String ipAddress) throws IOException {
            final InetAddress ip = InetAddresses.forString(ipAddress);
            final DatabaseRecord<RESPONSE> record = reader.getRecord(ip, clazz);
            final RESPONSE data = record.getData();
            return (data == null) ? null : new Result<>(data, NetworkAddress.format(ip), record.getNetwork().toString());
        }

        /**
         * Extract the configured properties from the retrieved response
         * @param response the non-null response that was retrieved
         * @return a mapping of properties for the ip from the response
         */
        protected abstract Map<String, Object> transform(Result<RESPONSE> response);
    }
}
