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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A collection of {@link IpDataLookup} implementations for IPinfo databases
 */
final class IpinfoIpDataLookups {

    private IpinfoIpDataLookups() {
        // utility class
    }

    private static final Logger logger = LogManager.getLogger(IpinfoIpDataLookups.class);

    // the actual prefix from the metadata is cased like the literal string, and
    // prefix dispatch and checks case-insensitive, so that works out nicely
    static final String IPINFO_PREFIX = "ipinfo";

    private static final Set<String> IPINFO_TYPE_STOP_WORDS = Set.of(
        "ipinfo",
        "extended",
        "free",
        "generic",
        "ip",
        "sample",
        "standard",
        "mmdb"
    );

    /**
     * Cleans up the database_type String from an ipinfo database by splitting on punctuation, removing stop words, and then joining
     * with an underscore.
     * <p>
     * e.g. "ipinfo free_foo_sample.mmdb" -> "foo"
     *
     * @param type the database_type from an ipinfo database
     * @return a cleaned up database_type string
     */
    // n.b. this is just based on observation of the types from a survey of such databases -- it's like browser user agent sniffing,
    // there aren't necessarily any amazing guarantees about this behavior
    static String ipinfoTypeCleanup(String type) {
        List<String> parts = Arrays.asList(type.split("[ _.]"));
        return parts.stream().filter((s) -> IPINFO_TYPE_STOP_WORDS.contains(s) == false).collect(Collectors.joining("_"));
    }

    @Nullable
    static Database getIpinfoDatabase(final String databaseType) {
        // for ipinfo the database selection is more along the lines of user-agent sniffing than
        // string-based dispatch. the specific database_type strings could change in the future,
        // hence the somewhat loose nature of this checking.

        final String cleanedType = ipinfoTypeCleanup(databaseType);

        // early detection on any of the 'extended' types
        if (databaseType.contains("extended")) {
            // which are not currently supported
            logger.trace("returning null for unsupported database_type [{}]", databaseType);
            return null;
        }

        // early detection on 'country_asn' so the 'country' and 'asn' checks don't get faked out
        if (cleanedType.contains("country_asn")) {
            // but it's not currently supported
            logger.trace("returning null for unsupported database_type [{}]", databaseType);
            return null;
        }

        if (cleanedType.contains("asn")) {
            return Database.AsnV2;
        } else if (cleanedType.contains("country")) {
            return Database.CountryV2;
        } else if (cleanedType.contains("location")) { // note: catches 'location' and 'geolocation' ;)
            return Database.CityV2;
        } else if (cleanedType.contains("privacy")) {
            return Database.PrivacyDetection;
        } else {
            // no match was found
            logger.trace("returning null for unsupported database_type [{}]", databaseType);
            return null;
        }
    }

    @Nullable
    static Function<Set<Database.Property>, IpDataLookup> getIpinfoLookup(final Database database) {
        return switch (database) {
            case AsnV2 -> IpinfoIpDataLookups.Asn::new;
            case CountryV2 -> IpinfoIpDataLookups.Country::new;
            case CityV2 -> IpinfoIpDataLookups.Geolocation::new;
            case PrivacyDetection -> IpinfoIpDataLookups.PrivacyDetection::new;
            default -> null;
        };
    }

    /**
     * Lax-ly parses a string that (ideally) looks like 'AS123' into a Long like 123L (or null, if such parsing isn't possible).
     * @param asn a potentially empty (or null) ASN string that is expected to contain 'AS' and then a parsable long
     * @return the parsed asn
     */
    static Long parseAsn(final String asn) {
        if (asn == null || Strings.hasText(asn) == false) {
            return null;
        } else {
            String stripped = asn.toUpperCase(Locale.ROOT).replaceAll("AS", "").trim();
            try {
                return Long.parseLong(stripped);
            } catch (NumberFormatException e) {
                logger.trace("Unable to parse non-compliant ASN string [{}]", asn);
                return null;
            }
        }
    }

    /**
     * Lax-ly parses a string that contains a boolean into a Boolean (or null, if such parsing isn't possible).
     * @param bool a potentially empty (or null) string that is expected to contain a parsable boolean
     * @return the parsed boolean
     */
    static Boolean parseBoolean(final String bool) {
        if (bool == null) {
            return null;
        } else {
            String trimmed = bool.toLowerCase(Locale.ROOT).trim();
            if ("true".equals(trimmed)) {
                return true;
            } else if ("false".equals(trimmed)) {
                // "false" can represent false -- this an expected future enhancement in how the database represents booleans
                return false;
            } else if (trimmed.isEmpty()) {
                // empty string can represent false -- this is how the database currently represents 'false' values
                return false;
            } else {
                logger.trace("Unable to parse non-compliant boolean string [{}]", bool);
                return null;
            }
        }
    }

    /**
     * Lax-ly parses a string that contains a double into a Double (or null, if such parsing isn't possible).
     * @param latlon a potentially empty (or null) string that is expected to contain a parsable double
     * @return the parsed double
     */
    static Double parseLocationDouble(final String latlon) {
        if (latlon == null || Strings.hasText(latlon) == false) {
            return null;
        } else {
            String stripped = latlon.trim();
            try {
                return Double.parseDouble(stripped);
            } catch (NumberFormatException e) {
                logger.trace("Unable to parse non-compliant location string [{}]", latlon);
                return null;
            }
        }
    }

    public record AsnResult(
        Long asn,
        @Nullable String country, // not present in the free asn database
        String domain,
        String name,
        @Nullable String type // not present in the free asn database
    ) {
        @SuppressWarnings("checkstyle:RedundantModifier")
        @MaxMindDbConstructor
        public AsnResult(
            @MaxMindDbParameter(name = "asn") String asn,
            @Nullable @MaxMindDbParameter(name = "country") String country,
            @MaxMindDbParameter(name = "domain") String domain,
            @MaxMindDbParameter(name = "name") String name,
            @Nullable @MaxMindDbParameter(name = "type") String type
        ) {
            this(parseAsn(asn), country, domain, name, type);
        }
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

    public record GeolocationResult(
        String city,
        String country,
        Double lat,
        Double lng,
        String postalCode,
        String region,
        String timezone
    ) {
        @SuppressWarnings("checkstyle:RedundantModifier")
        @MaxMindDbConstructor
        public GeolocationResult(
            @MaxMindDbParameter(name = "city") String city,
            @MaxMindDbParameter(name = "country") String country,
            // @MaxMindDbParameter(name = "geoname_id") String geonameId, // for now we're not exposing this
            @MaxMindDbParameter(name = "lat") String lat,
            @MaxMindDbParameter(name = "lng") String lng,
            @MaxMindDbParameter(name = "postal_code") String postalCode,
            @MaxMindDbParameter(name = "region") String region,
            // @MaxMindDbParameter(name = "region_code") String regionCode, // for now we're not exposing this
            @MaxMindDbParameter(name = "timezone") String timezone
        ) {
            this(city, country, parseLocationDouble(lat), parseLocationDouble(lng), postalCode, region, timezone);
        }
    }

    public record PrivacyDetectionResult(Boolean hosting, Boolean proxy, Boolean relay, String service, Boolean tor, Boolean vpn) {
        @SuppressWarnings("checkstyle:RedundantModifier")
        @MaxMindDbConstructor
        public PrivacyDetectionResult(
            @MaxMindDbParameter(name = "hosting") String hosting,
            // @MaxMindDbParameter(name = "network") String network, // for now we're not exposing this
            @MaxMindDbParameter(name = "proxy") String proxy,
            @MaxMindDbParameter(name = "relay") String relay,
            @MaxMindDbParameter(name = "service") String service, // n.b. this remains a string, the rest are parsed as booleans
            @MaxMindDbParameter(name = "tor") String tor,
            @MaxMindDbParameter(name = "vpn") String vpn
        ) {
            this(parseBoolean(hosting), parseBoolean(proxy), parseBoolean(relay), service, parseBoolean(tor), parseBoolean(vpn));
        }
    }

    static class Asn extends AbstractBase<AsnResult> {
        Asn(Set<Database.Property> properties) {
            super(properties, AsnResult.class);
        }

        @Override
        protected Map<String, Object> transform(final Result<AsnResult> result) {
            AsnResult response = result.result;
            Long asn = response.asn;
            String organizationName = response.name;
            String network = result.network;

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", result.ip);
                    case ASN -> {
                        if (asn != null) {
                            data.put("asn", asn);
                        }
                    }
                    case ORGANIZATION_NAME -> {
                        if (organizationName != null) {
                            data.put("organization_name", organizationName);
                        }
                    }
                    case NETWORK -> {
                        if (network != null) {
                            data.put("network", network);
                        }
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.country != null) {
                            data.put("country_iso_code", response.country);
                        }
                    }
                    case DOMAIN -> {
                        if (response.domain != null) {
                            data.put("domain", response.domain);
                        }
                    }
                    case TYPE -> {
                        if (response.type != null) {
                            data.put("type", response.type);
                        }
                    }
                }
            }
            return data;
        }
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

    static class Geolocation extends AbstractBase<GeolocationResult> {
        Geolocation(final Set<Database.Property> properties) {
            super(properties, GeolocationResult.class);
        }

        @Override
        protected Map<String, Object> transform(final Result<GeolocationResult> result) {
            GeolocationResult response = result.result;

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
                    case REGION_NAME -> {
                        String subdivisionName = response.region;
                        if (subdivisionName != null) {
                            data.put("region_name", subdivisionName);
                        }
                    }
                    case CITY_NAME -> {
                        String cityName = response.city;
                        if (cityName != null) {
                            data.put("city_name", cityName);
                        }
                    }
                    case TIMEZONE -> {
                        String locationTimeZone = response.timezone;
                        if (locationTimeZone != null) {
                            data.put("timezone", locationTimeZone);
                        }
                    }
                    case POSTAL_CODE -> {
                        String postalCode = response.postalCode;
                        if (postalCode != null) {
                            data.put("postal_code", postalCode);
                        }
                    }
                    case LOCATION -> {
                        Double latitude = response.lat;
                        Double longitude = response.lng;
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = new HashMap<>();
                            locationObject.put("lat", latitude);
                            locationObject.put("lon", longitude);
                            data.put("location", locationObject);
                        }
                    }
                }
            }
            return data;
        }
    }

    static class PrivacyDetection extends AbstractBase<PrivacyDetectionResult> {
        PrivacyDetection(Set<Database.Property> properties) {
            super(properties, PrivacyDetectionResult.class);
        }

        @Override
        protected Map<String, Object> transform(final Result<PrivacyDetectionResult> result) {
            PrivacyDetectionResult response = result.result;

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", result.ip);
                    case HOSTING -> {
                        if (response.hosting != null) {
                            data.put("hosting", response.hosting);
                        }
                    }
                    case TOR -> {
                        if (response.tor != null) {
                            data.put("tor", response.tor);
                        }
                    }
                    case PROXY -> {
                        if (response.proxy != null) {
                            data.put("proxy", response.proxy);
                        }
                    }
                    case RELAY -> {
                        if (response.relay != null) {
                            data.put("relay", response.relay);
                        }
                    }
                    case VPN -> {
                        if (response.vpn != null) {
                            data.put("vpn", response.vpn);
                        }
                    }
                    case SERVICE -> {
                        if (Strings.hasText(response.service)) {
                            data.put("service", response.service);
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
