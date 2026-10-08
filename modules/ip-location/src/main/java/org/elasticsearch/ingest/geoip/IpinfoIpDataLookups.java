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
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A collection of {@link InternalIpDataLookup} implementations for IPinfo databases
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

        // Note that 'plus' must be checked first because plus database types can include other type names, like `bundle_location_plus`.
        if (cleanedType.contains("plus")) {
            return Database.IpinfoPlus;
        } else if (cleanedType.contains("asn")) {
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
    static Function<Set<DatabaseProperty>, InternalIpDataLookup> getIpinfoLookup(final Database database) {
        return switch (database) {
            case AsnV2 -> IpinfoIpDataLookups.Asn::new;
            case CountryV2 -> IpinfoIpDataLookups.Country::new;
            case CityV2 -> IpinfoIpDataLookups.Geolocation::new;
            case PrivacyDetection -> IpinfoIpDataLookups.PrivacyDetection::new;
            case IpinfoPlus -> IpinfoIpDataLookups.Plus::new;
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

    public record PlusResult(
        Long asn,
        String asName,
        String asDomain,
        String asType,
        String carrierName,
        String mcc,
        String mnc,
        String asChanged,
        String geoChanged,
        String city,
        String region,
        String regionCode,
        String country,
        String countryCode,
        String continent,
        String continentCode,
        Double latitude,
        Double longitude,
        String timezone,
        String postalCode,
        String dmaCode,
        String geonameId,
        Integer radius,
        Boolean isAnonymous,
        Boolean isAnycast,
        Boolean isHosting,
        Boolean isMobile,
        Boolean isSatellite,
        Boolean isProxy,
        Boolean isRelay,
        Boolean isTor,
        Boolean isVpn,
        String privacyName
    ) {
        @SuppressWarnings("checkstyle:RedundantModifier")
        @MaxMindDbConstructor
        public PlusResult(
            @MaxMindDbParameter(name = "asn") String asn,
            @MaxMindDbParameter(name = "as_name") String asName,
            @MaxMindDbParameter(name = "as_domain") String asDomain,
            @MaxMindDbParameter(name = "as_type") String asType,
            @MaxMindDbParameter(name = "carrier_name") String carrierName,
            @MaxMindDbParameter(name = "mcc") String mcc,
            @MaxMindDbParameter(name = "mnc") String mnc,
            @MaxMindDbParameter(name = "as_changed") String asChanged,
            @MaxMindDbParameter(name = "geo_changed") String geoChanged,
            @MaxMindDbParameter(name = "city") String city,
            @MaxMindDbParameter(name = "region") String region,
            @MaxMindDbParameter(name = "region_code") String regionCode,
            @MaxMindDbParameter(name = "country") String country,
            @MaxMindDbParameter(name = "country_code") String countryCode,
            @MaxMindDbParameter(name = "continent") String continent,
            @MaxMindDbParameter(name = "continent_code") String continentCode,
            @MaxMindDbParameter(name = "latitude") Double latitude,
            @MaxMindDbParameter(name = "longitude") Double longitude,
            @MaxMindDbParameter(name = "timezone") String timezone,
            @MaxMindDbParameter(name = "postal_code") String postalCode,
            @MaxMindDbParameter(name = "dma_code") String dmaCode,
            @MaxMindDbParameter(name = "geoname_id") String geonameId,
            @MaxMindDbParameter(name = "radius") Integer radius,
            @MaxMindDbParameter(name = "is_anonymous") Boolean isAnonymous,
            @MaxMindDbParameter(name = "is_anycast") Boolean isAnycast,
            @MaxMindDbParameter(name = "is_hosting") Boolean isHosting,
            @MaxMindDbParameter(name = "is_mobile") Boolean isMobile,
            @MaxMindDbParameter(name = "is_satellite") Boolean isSatellite,
            @MaxMindDbParameter(name = "is_proxy") Boolean isProxy,
            @MaxMindDbParameter(name = "is_relay") Boolean isRelay,
            @MaxMindDbParameter(name = "is_tor") Boolean isTor,
            @MaxMindDbParameter(name = "is_vpn") Boolean isVpn,
            @MaxMindDbParameter(name = "privacy_name") String privacyName
        ) {
            this(
                parseAsn(asn),
                asName,
                asDomain,
                asType,
                carrierName,
                mcc,
                mnc,
                asChanged,
                geoChanged,
                city,
                region,
                regionCode,
                country,
                countryCode,
                continent,
                continentCode,
                latitude,
                longitude,
                timezone,
                postalCode,
                dmaCode,
                geonameId,
                radius,
                isAnonymous,
                isAnycast,
                isHosting,
                isMobile,
                isSatellite,
                isProxy,
                isRelay,
                isTor,
                isVpn,
                privacyName
            );
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
        Asn(Set<DatabaseProperty> properties) {
            super(properties, AsnResult.class);
        }

        @Override
        protected void transform(final Result<AsnResult> result, final IpLocationInfoCollector collector) {
            AsnResult response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case ASN -> {
                        if (response.asn != null) collector.asn(response.asn);
                    }
                    case ORGANIZATION_NAME -> {
                        if (response.name != null) collector.organizationName(response.name);
                    }
                    case NETWORK -> {
                        if (result.network() != null) collector.network(result.network());
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.country != null) collector.countryIsoCode(response.country);
                    }
                    case DOMAIN -> {
                        if (response.domain != null) collector.domain(response.domain);
                    }
                    case TYPE -> {
                        if (response.type != null) collector.type(response.type);
                    }
                }
            }
        }
    }

    static class Country extends AbstractBase<CountryResult> {
        Country(Set<DatabaseProperty> properties) {
            super(properties, CountryResult.class);
        }

        @Override
        protected void transform(final Result<CountryResult> result, final IpLocationInfoCollector collector) {
            CountryResult response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case COUNTRY_ISO_CODE -> {
                        if (response.country != null) collector.countryIsoCode(response.country);
                    }
                    case COUNTRY_NAME -> {
                        if (response.countryName != null) collector.countryName(response.countryName);
                    }
                    case CONTINENT_CODE -> {
                        if (response.continent != null) collector.continentCode(response.continent);
                    }
                    case CONTINENT_NAME -> {
                        if (response.continentName != null) collector.continentName(response.continentName);
                    }
                }
            }
        }
    }

    static class Geolocation extends AbstractBase<GeolocationResult> {
        Geolocation(final Set<DatabaseProperty> properties) {
            super(properties, GeolocationResult.class);
        }

        @Override
        protected void transform(final Result<GeolocationResult> result, final IpLocationInfoCollector collector) {
            GeolocationResult response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case COUNTRY_ISO_CODE -> {
                        if (response.country != null) collector.countryIsoCode(response.country);
                    }
                    case REGION_NAME -> {
                        if (response.region != null) collector.regionName(response.region);
                    }
                    case CITY_NAME -> {
                        if (response.city != null) collector.cityName(response.city);
                    }
                    case TIMEZONE -> {
                        if (response.timezone != null) collector.timezone(response.timezone);
                    }
                    case POSTAL_CODE -> {
                        if (response.postalCode != null) collector.postalCode(response.postalCode);
                    }
                    case LOCATION -> {
                        if (response.lat != null && response.lng != null) {
                            collector.location(response.lat, response.lng);
                        }
                    }
                }
            }
        }
    }

    static class PrivacyDetection extends AbstractBase<PrivacyDetectionResult> {
        PrivacyDetection(Set<DatabaseProperty> properties) {
            super(properties, PrivacyDetectionResult.class);
        }

        @Override
        protected void transform(final Result<PrivacyDetectionResult> result, final IpLocationInfoCollector collector) {
            PrivacyDetectionResult response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case HOSTING -> {
                        if (response.hosting != null) collector.hosting(response.hosting);
                    }
                    case TOR -> {
                        if (response.tor != null) collector.tor(response.tor);
                    }
                    case PROXY -> {
                        if (response.proxy != null) collector.proxy(response.proxy);
                    }
                    case RELAY -> {
                        if (response.relay != null) collector.relay(response.relay);
                    }
                    case VPN -> {
                        if (response.vpn != null) collector.vpn(response.vpn);
                    }
                    case SERVICE -> {
                        if (Strings.hasText(response.service)) collector.service(response.service);
                    }
                }
            }
        }
    }

    static class Plus extends AbstractBase<PlusResult> {
        Plus(Set<DatabaseProperty> properties) {
            super(properties, PlusResult.class);
        }

        @Override
        protected void transform(final Result<PlusResult> result, final IpLocationInfoCollector collector) {
            PlusResult response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case NETWORK -> {
                        if (result.network() != null) collector.network(result.network());
                    }
                    case CITY_NAME -> {
                        if (Strings.hasText(response.city)) collector.cityName(response.city);
                    }
                    case REGION_NAME -> {
                        if (Strings.hasText(response.region)) collector.regionName(response.region);
                    }
                    case REGION_ISO_CODE -> {
                        if (Strings.hasText(response.regionCode)) collector.regionIsoCode(response.regionCode);
                    }
                    case COUNTRY_NAME -> {
                        if (Strings.hasText(response.country)) collector.countryName(response.country);
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (Strings.hasText(response.countryCode)) collector.countryIsoCode(response.countryCode);
                    }
                    case CONTINENT_NAME -> {
                        if (Strings.hasText(response.continent)) collector.continentName(response.continent);
                    }
                    case CONTINENT_CODE -> {
                        if (Strings.hasText(response.continentCode)) collector.continentCode(response.continentCode);
                    }
                    case LOCATION -> {
                        if (response.latitude != null && response.longitude != null) {
                            collector.location(response.latitude, response.longitude);
                        }
                    }
                    case TIMEZONE -> {
                        if (Strings.hasText(response.timezone)) collector.timezone(response.timezone);
                    }
                    case POSTAL_CODE -> {
                        if (Strings.hasText(response.postalCode)) collector.postalCode(response.postalCode);
                    }
                    case DMA_CODE -> {
                        if (Strings.hasText(response.dmaCode)) collector.dmaCode(response.dmaCode);
                    }
                    case GEONAME_ID -> {
                        if (Strings.hasText(response.geonameId)) collector.geonameId(response.geonameId);
                    }
                    case ACCURACY_RADIUS -> {
                        if (response.radius != null) collector.accuracyRadius(response.radius);
                    }
                    case ASN -> {
                        if (response.asn != null) collector.asn(response.asn);
                    }
                    case ORGANIZATION_NAME -> {
                        if (Strings.hasText(response.asName)) collector.organizationName(response.asName);
                    }
                    case DOMAIN -> {
                        if (Strings.hasText(response.asDomain)) collector.domain(response.asDomain);
                    }
                    case TYPE -> {
                        if (Strings.hasText(response.asType)) collector.type(response.asType);
                    }
                    case ISP -> {
                        if (Strings.hasText(response.carrierName)) collector.isp(response.carrierName);
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (Strings.hasText(response.mcc)) collector.mobileCountryCode(response.mcc);
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (Strings.hasText(response.mnc)) collector.mobileNetworkCode(response.mnc);
                    }
                    case ASN_CHANGED_DATE -> {
                        if (Strings.hasText(response.asChanged)) collector.asnChangedDate(response.asChanged);
                    }
                    case GEO_CHANGED_DATE -> {
                        if (Strings.hasText(response.geoChanged)) collector.geoChangedDate(response.geoChanged);
                    }
                    case ANONYMOUS -> {
                        if (response.isAnonymous != null) collector.anonymous(response.isAnonymous);
                    }
                    case ANYCAST -> {
                        if (response.isAnycast != null) collector.anycast(response.isAnycast);
                    }
                    case HOSTING -> {
                        if (response.isHosting != null) collector.hosting(response.isHosting);
                    }
                    case MOBILE -> {
                        if (response.isMobile != null) collector.mobile(response.isMobile);
                    }
                    case SATELLITE -> {
                        if (response.isSatellite != null) collector.satellite(response.isSatellite);
                    }
                    case PROXY -> {
                        if (response.isProxy != null) collector.proxy(response.isProxy);
                    }
                    case RELAY -> {
                        if (response.isRelay != null) collector.relay(response.isRelay);
                    }
                    case TOR -> {
                        if (response.isTor != null) collector.tor(response.isTor);
                    }
                    case VPN -> {
                        if (response.isVpn != null) collector.vpn(response.isVpn);
                    }
                    case SERVICE -> {
                        if (Strings.hasText(response.privacyName)) collector.service(response.privacyName);
                    }
                }
            }
        }
    }

    /**
     * The {@link IpinfoIpDataLookups.AbstractBase} is an abstract base implementation of {@link InternalIpDataLookup} that
     * provides common functionality for getting a {@link InternalIpDataLookup.Result} that wraps a record from a {@link IpDatabase}.
     *
     * @param <RESPONSE> the record type that will be wrapped and returned
     */
    private abstract static class AbstractBase<RESPONSE> implements InternalIpDataLookup {

        protected final Set<DatabaseProperty> properties;
        protected final Class<RESPONSE> clazz;

        AbstractBase(final Set<DatabaseProperty> properties, final Class<RESPONSE> clazz) {
            this.properties = Set.copyOf(properties);
            this.clazz = clazz;
        }

        @Override
        public Set<DatabaseProperty> getProperties() {
            return this.properties;
        }

        @Override
        public final boolean getData(final IpDatabase ipDatabase, final String ipAddress, final IpLocationInfoCollector collector) {
            final Result<RESPONSE> response = ipDatabase.getResponse(ipAddress, this::lookup);
            if (response == null || response.result() == null) {
                return false;
            }
            transform(response, collector);
            return true;
        }

        @Nullable
        private Result<RESPONSE> lookup(final Reader reader, final String ipAddress) throws IOException {
            final InetAddress ip = InetAddresses.forString(ipAddress);
            final DatabaseRecord<RESPONSE> entry = reader.getRecord(ip, clazz);
            final RESPONSE data = entry.getData();
            return (data == null) ? null : new Result<>(data, NetworkAddress.format(ip), entry.getNetwork().toString());
        }

        /**
         * Extract the configured properties from the retrieved response and push them to the collector.
         *
         * @param response the non-null response that was retrieved
         * @param collector the collector to push results to
         */
        protected abstract void transform(Result<RESPONSE> response, IpLocationInfoCollector collector);
    }
}
