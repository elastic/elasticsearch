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
import com.maxmind.db.Network;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.model.AbstractResponse;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.EnterpriseResponse;
import com.maxmind.geoip2.model.IspResponse;
import com.maxmind.geoip2.record.Continent;
import com.maxmind.geoip2.record.Location;
import com.maxmind.geoip2.record.Postal;
import com.maxmind.geoip2.record.Subdivision;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * A collection of {@link IpDataLookup} implementations for MaxMind databases
 */
final class MaxmindIpDataLookups {

    private MaxmindIpDataLookups() {
        // utility class
    }

    private static final Logger logger = LogManager.getLogger(MaxmindIpDataLookups.class);

    // the actual prefixes from the metadata are cased like the literal strings, but
    // prefix dispatch and checks case-insensitive, so the actual constants are lowercase
    static final String GEOIP2_PREFIX = "GeoIP2".toLowerCase(Locale.ROOT);
    static final String GEOLITE2_PREFIX = "GeoLite2".toLowerCase(Locale.ROOT);

    // note: the secondary dispatch on suffix happens to be case sensitive
    private static final String CITY_DB_SUFFIX = "-City";
    private static final String COUNTRY_DB_SUFFIX = "-Country";
    private static final String ASN_DB_SUFFIX = "-ASN";
    private static final String ANONYMOUS_IP_DB_SUFFIX = "-Anonymous-IP";
    private static final String CONNECTION_TYPE_DB_SUFFIX = "-Connection-Type";
    private static final String DOMAIN_DB_SUFFIX = "-Domain";
    private static final String ENTERPRISE_DB_SUFFIX = "-Enterprise";
    private static final String ISP_DB_SUFFIX = "-ISP";

    @Nullable
    static Database getMaxmindDatabase(final String databaseType) {
        if (databaseType.endsWith(CITY_DB_SUFFIX)) {
            return Database.City;
        } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
            return Database.Country;
        } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
            return Database.Asn;
        } else if (databaseType.endsWith(ANONYMOUS_IP_DB_SUFFIX)) {
            return Database.AnonymousIp;
        } else if (databaseType.endsWith(CONNECTION_TYPE_DB_SUFFIX)) {
            return Database.ConnectionType;
        } else if (databaseType.endsWith(DOMAIN_DB_SUFFIX)) {
            return Database.Domain;
        } else if (databaseType.endsWith(ENTERPRISE_DB_SUFFIX)) {
            return Database.Enterprise;
        } else if (databaseType.endsWith(ISP_DB_SUFFIX)) {
            return Database.Isp;
        } else {
            // no match was found
            logger.trace("returning null for unsupported database_type [{}]", databaseType);
            return null;
        }
    }

    @Nullable
    static Function<Set<Database.Property>, IpDataLookup> getMaxmindLookup(final Database database) {
        return switch (database) {
            case City -> MaxmindIpDataLookups.City::new;
            case Country -> MaxmindIpDataLookups.Country::new;
            case Asn -> MaxmindIpDataLookups.Asn::new;
            case AnonymousIp -> MaxmindIpDataLookups.AnonymousIp::new;
            case ConnectionType -> MaxmindIpDataLookups.ConnectionType::new;
            case Domain -> MaxmindIpDataLookups.Domain::new;
            case Enterprise -> MaxmindIpDataLookups.Enterprise::new;
            case Isp -> MaxmindIpDataLookups.Isp::new;
            default -> null;
        };
    }

    static class AnonymousIp extends AbstractBase<AnonymousIpResponse> {
        AnonymousIp(final Set<Database.Property> properties) {
            super(
                properties,
                AnonymousIpResponse.class,
                (response, ipAddress, network, locales) -> new AnonymousIpResponse(response, ipAddress, network)
            );
        }

        @Override
        protected Map<String, Object> transform(final AnonymousIpResponse response) {
            boolean isHostingProvider = response.isHostingProvider();
            boolean isTorExitNode = response.isTorExitNode();
            boolean isAnonymousVpn = response.isAnonymousVpn();
            boolean isAnonymous = response.isAnonymous();
            boolean isPublicProxy = response.isPublicProxy();
            boolean isResidentialProxy = response.isResidentialProxy();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getIpAddress());
                    case HOSTING_PROVIDER -> {
                        data.put("hosting_provider", isHostingProvider);
                    }
                    case TOR_EXIT_NODE -> {
                        data.put("tor_exit_node", isTorExitNode);
                    }
                    case ANONYMOUS_VPN -> {
                        data.put("anonymous_vpn", isAnonymousVpn);
                    }
                    case ANONYMOUS -> {
                        data.put("anonymous", isAnonymous);
                    }
                    case PUBLIC_PROXY -> {
                        data.put("public_proxy", isPublicProxy);
                    }
                    case RESIDENTIAL_PROXY -> {
                        data.put("residential_proxy", isResidentialProxy);
                    }
                }
            }
            return data;
        }
    }

    static class Asn extends AbstractBase<AsnResponse> {
        Asn(Set<Database.Property> properties) {
            super(properties, AsnResponse.class, (response, ipAddress, network, locales) -> new AsnResponse(response, ipAddress, network));
        }

        @Override
        protected Map<String, Object> transform(final AsnResponse response) {
            Long asn = response.getAutonomousSystemNumber();
            String organizationName = response.getAutonomousSystemOrganization();
            Network network = response.getNetwork();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getIpAddress());
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
                            data.put("network", network.toString());
                        }
                    }
                }
            }
            return data;
        }
    }

    static class City extends AbstractBase<CityResponse> {
        City(final Set<Database.Property> properties) {
            super(properties, CityResponse.class, CityResponse::new);
        }

        @Override
        protected Map<String, Object> transform(final CityResponse response) {
            com.maxmind.geoip2.record.Country country = response.getCountry();
            com.maxmind.geoip2.record.Country registeredCountry = response.getRegisteredCountry();
            com.maxmind.geoip2.record.City city = response.getCity();
            Location location = response.getLocation();
            Continent continent = response.getContinent();
            Subdivision subdivision = response.getMostSpecificSubdivision();
            Postal postal = response.getPostal();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getTraits().getIpAddress());
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (country.getIsoCode() != null) {
                            // isInEuropeanUnion is a boolean so it can't be null. But it really only makes sense if we have a country
                            data.put("country_in_european_union", country.isInEuropeanUnion());
                        }
                    }
                    case COUNTRY_ISO_CODE -> {
                        String countryIsoCode = country.getIsoCode();
                        if (countryIsoCode != null) {
                            data.put("country_iso_code", countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        String countryName = country.getName();
                        if (countryName != null) {
                            data.put("country_name", countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        String continentCode = continent.getCode();
                        if (continentCode != null) {
                            data.put("continent_code", continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        String continentName = continent.getName();
                        if (continentName != null) {
                            data.put("continent_name", continentName);
                        }
                    }
                    case REGION_ISO_CODE -> {
                        // ISO 3166-2 code for country subdivisions.
                        // See iso.org/iso-3166-country-codes.html
                        String countryIso = country.getIsoCode();
                        String subdivisionIso = subdivision.getIsoCode();
                        if (countryIso != null && subdivisionIso != null) {
                            String regionIsoCode = countryIso + "-" + subdivisionIso;
                            data.put("region_iso_code", regionIsoCode);
                        }
                    }
                    case REGION_NAME -> {
                        String subdivisionName = subdivision.getName();
                        if (subdivisionName != null) {
                            data.put("region_name", subdivisionName);
                        }
                    }
                    case CITY_NAME -> {
                        String cityName = city.getName();
                        if (cityName != null) {
                            data.put("city_name", cityName);
                        }
                    }
                    case TIMEZONE -> {
                        String locationTimeZone = location.getTimeZone();
                        if (locationTimeZone != null) {
                            data.put("timezone", locationTimeZone);
                        }
                    }
                    case LOCATION -> {
                        Double latitude = location.getLatitude();
                        Double longitude = location.getLongitude();
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = new HashMap<>();
                            locationObject.put("lat", latitude);
                            locationObject.put("lon", longitude);
                            data.put("location", locationObject);
                        }
                    }
                    case ACCURACY_RADIUS -> {
                        Integer accuracyRadius = location.getAccuracyRadius();
                        if (accuracyRadius != null) {
                            data.put("accuracy_radius", accuracyRadius);
                        }
                    }
                    case POSTAL_CODE -> {
                        if (postal != null && postal.getCode() != null) {
                            data.put("postal_code", postal.getCode());
                        }
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (registeredCountry.getIsoCode() != null) {
                            // isInEuropeanUnion is a boolean so it can't be null. But it really only makes sense if we have a country
                            data.put("registered_country_in_european_union", registeredCountry.isInEuropeanUnion());
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (registeredCountry.getIsoCode() != null) {
                            data.put("registered_country_iso_code", registeredCountry.getIsoCode());
                        }
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (registeredCountry.getName() != null) {
                            data.put("registered_country_name", registeredCountry.getName());
                        }
                    }
                }
            }
            return data;
        }
    }

    static class ConnectionType extends AbstractBase<ConnectionTypeResponse> {
        ConnectionType(final Set<Database.Property> properties) {
            super(
                properties,
                ConnectionTypeResponse.class,
                (response, ipAddress, network, locales) -> new ConnectionTypeResponse(response, ipAddress, network)
            );
        }

        @Override
        protected Map<String, Object> transform(final ConnectionTypeResponse response) {
            ConnectionTypeResponse.ConnectionType connectionType = response.getConnectionType();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getIpAddress());
                    case CONNECTION_TYPE -> {
                        if (connectionType != null) {
                            data.put("connection_type", connectionType.toString());
                        }
                    }
                }
            }
            return data;
        }
    }

    static class Country extends AbstractBase<CountryResponse> {
        Country(final Set<Database.Property> properties) {
            super(properties, CountryResponse.class, CountryResponse::new);
        }

        @Override
        protected Map<String, Object> transform(final CountryResponse response) {
            com.maxmind.geoip2.record.Country country = response.getCountry();
            com.maxmind.geoip2.record.Country registeredCountry = response.getRegisteredCountry();
            Continent continent = response.getContinent();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getTraits().getIpAddress());
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (country.getIsoCode() != null) {
                            // isInEuropeanUnion is a boolean so it can't be null. But it really only makes sense if we have a country
                            data.put("country_in_european_union", country.isInEuropeanUnion());
                        }
                    }
                    case COUNTRY_ISO_CODE -> {
                        String countryIsoCode = country.getIsoCode();
                        if (countryIsoCode != null) {
                            data.put("country_iso_code", countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        String countryName = country.getName();
                        if (countryName != null) {
                            data.put("country_name", countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        String continentCode = continent.getCode();
                        if (continentCode != null) {
                            data.put("continent_code", continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        String continentName = continent.getName();
                        if (continentName != null) {
                            data.put("continent_name", continentName);
                        }
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (registeredCountry.getIsoCode() != null) {
                            // isInEuropeanUnion is a boolean so it can't be null. But it really only makes sense if we have a country
                            data.put("registered_country_in_european_union", registeredCountry.isInEuropeanUnion());
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (registeredCountry.getIsoCode() != null) {
                            data.put("registered_country_iso_code", registeredCountry.getIsoCode());
                        }
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (registeredCountry.getName() != null) {
                            data.put("registered_country_name", registeredCountry.getName());
                        }
                    }
                }
            }
            return data;
        }
    }

    static class Domain extends AbstractBase<DomainResponse> {
        Domain(final Set<Database.Property> properties) {
            super(
                properties,
                DomainResponse.class,
                (response, ipAddress, network, locales) -> new DomainResponse(response, ipAddress, network)
            );
        }

        @Override
        protected Map<String, Object> transform(final DomainResponse response) {
            String domain = response.getDomain();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getIpAddress());
                    case DOMAIN -> {
                        if (domain != null) {
                            data.put("domain", domain);
                        }
                    }
                }
            }
            return data;
        }
    }

    static class Enterprise extends AbstractBase<EnterpriseResponse> {
        Enterprise(final Set<Database.Property> properties) {
            super(properties, EnterpriseResponse.class, EnterpriseResponse::new);
        }

        @Override
        protected Map<String, Object> transform(final EnterpriseResponse response) {
            com.maxmind.geoip2.record.Country country = response.getCountry();
            com.maxmind.geoip2.record.Country registeredCountry = response.getRegisteredCountry();
            com.maxmind.geoip2.record.City city = response.getCity();
            Location location = response.getLocation();
            Continent continent = response.getContinent();
            Subdivision subdivision = response.getMostSpecificSubdivision();
            Postal postal = response.getPostal();

            Long asn = response.getTraits().getAutonomousSystemNumber();
            String organizationName = response.getTraits().getAutonomousSystemOrganization();
            Network network = response.getTraits().getNetwork();

            String isp = response.getTraits().getIsp();
            String ispOrganization = response.getTraits().getOrganization();
            String mobileCountryCode = response.getTraits().getMobileCountryCode();
            String mobileNetworkCode = response.getTraits().getMobileNetworkCode();

            boolean isHostingProvider = response.getTraits().isHostingProvider();
            boolean isTorExitNode = response.getTraits().isTorExitNode();
            boolean isAnonymousVpn = response.getTraits().isAnonymousVpn();
            boolean isAnonymous = response.getTraits().isAnonymous();
            boolean isPublicProxy = response.getTraits().isPublicProxy();
            boolean isResidentialProxy = response.getTraits().isResidentialProxy();

            String userType = response.getTraits().getUserType();

            String domain = response.getTraits().getDomain();

            ConnectionTypeResponse.ConnectionType connectionType = response.getTraits().getConnectionType();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getTraits().getIpAddress());
                    case COUNTRY_CONFIDENCE -> {
                        Integer countryConfidence = country.getConfidence();
                        if (countryConfidence != null) {
                            data.put("country_confidence", countryConfidence);
                        }
                    }
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (country.getIsoCode() != null) {
                            // isInEuropeanUnion is a boolean so it can't be null. But it really only makes sense if we have a country
                            data.put("country_in_european_union", country.isInEuropeanUnion());
                        }
                    }
                    case COUNTRY_ISO_CODE -> {
                        String countryIsoCode = country.getIsoCode();
                        if (countryIsoCode != null) {
                            data.put("country_iso_code", countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        String countryName = country.getName();
                        if (countryName != null) {
                            data.put("country_name", countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        String continentCode = continent.getCode();
                        if (continentCode != null) {
                            data.put("continent_code", continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        String continentName = continent.getName();
                        if (continentName != null) {
                            data.put("continent_name", continentName);
                        }
                    }
                    case REGION_ISO_CODE -> {
                        // ISO 3166-2 code for country subdivisions.
                        // See iso.org/iso-3166-country-codes.html
                        String countryIso = country.getIsoCode();
                        String subdivisionIso = subdivision.getIsoCode();
                        if (countryIso != null && subdivisionIso != null) {
                            String regionIsoCode = countryIso + "-" + subdivisionIso;
                            data.put("region_iso_code", regionIsoCode);
                        }
                    }
                    case REGION_NAME -> {
                        String subdivisionName = subdivision.getName();
                        if (subdivisionName != null) {
                            data.put("region_name", subdivisionName);
                        }
                    }
                    case CITY_CONFIDENCE -> {
                        Integer cityConfidence = city.getConfidence();
                        if (cityConfidence != null) {
                            data.put("city_confidence", cityConfidence);
                        }
                    }
                    case CITY_NAME -> {
                        String cityName = city.getName();
                        if (cityName != null) {
                            data.put("city_name", cityName);
                        }
                    }
                    case TIMEZONE -> {
                        String locationTimeZone = location.getTimeZone();
                        if (locationTimeZone != null) {
                            data.put("timezone", locationTimeZone);
                        }
                    }
                    case LOCATION -> {
                        Double latitude = location.getLatitude();
                        Double longitude = location.getLongitude();
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = new HashMap<>();
                            locationObject.put("lat", latitude);
                            locationObject.put("lon", longitude);
                            data.put("location", locationObject);
                        }
                    }
                    case ACCURACY_RADIUS -> {
                        Integer accuracyRadius = location.getAccuracyRadius();
                        if (accuracyRadius != null) {
                            data.put("accuracy_radius", accuracyRadius);
                        }
                    }
                    case POSTAL_CODE -> {
                        if (postal != null && postal.getCode() != null) {
                            data.put("postal_code", postal.getCode());
                        }
                    }
                    case POSTAL_CONFIDENCE -> {
                        Integer postalConfidence = postal.getConfidence();
                        if (postalConfidence != null) {
                            data.put("postal_confidence", postalConfidence);
                        }
                    }
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
                            data.put("network", network.toString());
                        }
                    }
                    case HOSTING_PROVIDER -> {
                        data.put("hosting_provider", isHostingProvider);
                    }
                    case TOR_EXIT_NODE -> {
                        data.put("tor_exit_node", isTorExitNode);
                    }
                    case ANONYMOUS_VPN -> {
                        data.put("anonymous_vpn", isAnonymousVpn);
                    }
                    case ANONYMOUS -> {
                        data.put("anonymous", isAnonymous);
                    }
                    case PUBLIC_PROXY -> {
                        data.put("public_proxy", isPublicProxy);
                    }
                    case RESIDENTIAL_PROXY -> {
                        data.put("residential_proxy", isResidentialProxy);
                    }
                    case DOMAIN -> {
                        if (domain != null) {
                            data.put("domain", domain);
                        }
                    }
                    case ISP -> {
                        if (isp != null) {
                            data.put("isp", isp);
                        }
                    }
                    case ISP_ORGANIZATION_NAME -> {
                        if (ispOrganization != null) {
                            data.put("isp_organization_name", ispOrganization);
                        }
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (mobileCountryCode != null) {
                            data.put("mobile_country_code", mobileCountryCode);
                        }
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (mobileNetworkCode != null) {
                            data.put("mobile_network_code", mobileNetworkCode);
                        }
                    }
                    case USER_TYPE -> {
                        if (userType != null) {
                            data.put("user_type", userType);
                        }
                    }
                    case CONNECTION_TYPE -> {
                        if (connectionType != null) {
                            data.put("connection_type", connectionType.toString());
                        }
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (registeredCountry.getIsoCode() != null) {
                            // isInEuropeanUnion is a boolean so it can't be null. But it really only makes sense if we have a country
                            data.put("registered_country_in_european_union", registeredCountry.isInEuropeanUnion());
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (registeredCountry.getIsoCode() != null) {
                            data.put("registered_country_iso_code", registeredCountry.getIsoCode());
                        }
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (registeredCountry.getName() != null) {
                            data.put("registered_country_name", registeredCountry.getName());
                        }
                    }
                }
            }
            return data;
        }
    }

    static class Isp extends AbstractBase<IspResponse> {
        Isp(final Set<Database.Property> properties) {
            super(properties, IspResponse.class, (response, ipAddress, network, locales) -> new IspResponse(response, ipAddress, network));
        }

        @Override
        protected Map<String, Object> transform(final IspResponse response) {
            String isp = response.getIsp();
            String ispOrganization = response.getOrganization();
            String mobileNetworkCode = response.getMobileNetworkCode();
            String mobileCountryCode = response.getMobileCountryCode();
            Long asn = response.getAutonomousSystemNumber();
            String organizationName = response.getAutonomousSystemOrganization();
            Network network = response.getNetwork();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", response.getIpAddress());
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
                            data.put("network", network.toString());
                        }
                    }
                    case ISP -> {
                        if (isp != null) {
                            data.put("isp", isp);
                        }
                    }
                    case ISP_ORGANIZATION_NAME -> {
                        if (ispOrganization != null) {
                            data.put("isp_organization_name", ispOrganization);
                        }
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (mobileCountryCode != null) {
                            data.put("mobile_country_code", mobileCountryCode);
                        }
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (mobileNetworkCode != null) {
                            data.put("mobile_network_code", mobileNetworkCode);
                        }
                    }
                }
            }
            return data;
        }
    }

    /**
     * As an internal detail, the {@code com.maxmind.geoip2.model } classes that are populated by
     * {@link Reader#getRecord(InetAddress, Class)} are kinda half-populated and need to go through a second round of construction
     * with context from the querying caller. This method gives us a place do that additional binding. Cleverly, the signature
     * here matches the constructor for many of these model classes exactly, so an appropriate implementation can 'just' be a method
     * reference in some cases (in other cases it needs to be a lambda).
     */
    @FunctionalInterface
    private interface ResponseBuilder<RESPONSE extends AbstractResponse> {
        RESPONSE build(RESPONSE resp, String address, Network network, List<String> locales);
    }

    /**
     * The {@link MaxmindIpDataLookups.AbstractBase} is an abstract base implementation of {@link IpDataLookup} that
     * provides common functionality for getting a specific kind of {@link AbstractResponse} from a {@link IpDatabase}.
     *
     * @param <RESPONSE> the intermediate type of {@link AbstractResponse}
     */
    private abstract static class AbstractBase<RESPONSE extends AbstractResponse> implements IpDataLookup {

        protected final Set<Database.Property> properties;
        protected final Class<RESPONSE> clazz;
        protected final ResponseBuilder<RESPONSE> builder;

        AbstractBase(final Set<Database.Property> properties, final Class<RESPONSE> clazz, final ResponseBuilder<RESPONSE> builder) {
            this.properties = Set.copyOf(properties);
            this.clazz = clazz;
            this.builder = builder;
        }

        @Override
        public Set<Database.Property> getProperties() {
            return this.properties;
        }

        @Override
        public final Map<String, Object> getData(final IpDatabase ipDatabase, final String ipAddress) {
            final RESPONSE response = ipDatabase.getResponse(ipAddress, this::lookup);
            return (response == null) ? Map.of() : transform(response);
        }

        @Nullable
        private RESPONSE lookup(final Reader reader, final String ipAddress) throws IOException {
            final InetAddress ip = InetAddresses.forString(ipAddress);
            final DatabaseRecord<RESPONSE> record = reader.getRecord(ip, clazz);
            final RESPONSE data = record.getData();
            return (data == null) ? null : builder.build(data, NetworkAddress.format(ip), record.getNetwork(), List.of("en"));
        }

        /**
         * Extract the configured properties from the retrieved response
         * @param response the non-null response that was retrieved
         * @return a mapping of properties for the ip from the response
         */
        protected abstract Map<String, Object> transform(RESPONSE response);
    }
}
