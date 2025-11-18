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
import com.maxmind.geoip2.record.Traits;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.ingest.geoip.IpDataLookup.Result;

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

    static class AnonymousIp extends AbstractBase<AnonymousIpResponse, AnonymousIpResponse> {
        AnonymousIp(final Set<Database.Property> properties) {
            super(
                properties,
                AnonymousIpResponse.class,
                (response, ipAddress, network, locales) -> new AnonymousIpResponse(response, ipAddress, network)
            );
        }

        @Override
        protected AnonymousIpResponse cacheableRecord(AnonymousIpResponse response) {
            return response;
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

    static class Asn extends AbstractBase<AsnResponse, AsnResponse> {
        Asn(Set<Database.Property> properties) {
            super(properties, AsnResponse.class, (response, ipAddress, network, locales) -> new AsnResponse(response, ipAddress, network));
        }

        @Override
        protected AsnResponse cacheableRecord(AsnResponse response) {
            return response;
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

    record CacheableCityResponse(
        Boolean isInEuropeanUnion,
        String countryIsoCode,
        String countryName,
        String continentCode,
        String continentName,
        String regionIsoCode,
        String regionName,
        String cityName,
        String timezone,
        Double latitude,
        Double longitude,
        Integer accuracyRadius,
        String postalCode,
        Boolean registeredCountryIsInEuropeanUnion,
        String registeredCountryIsoCode,
        String registeredCountryName
    ) {}

    static class City extends AbstractBase<CityResponse, Result<CacheableCityResponse>> {
        City(final Set<Database.Property> properties) {
            super(properties, CityResponse.class, CityResponse::new);
        }

        @Override
        protected Result<CacheableCityResponse> cacheableRecord(CityResponse response) {
            final com.maxmind.geoip2.record.Country country = response.getCountry();
            final Continent continent = response.getContinent();
            final Subdivision subdivision = response.getMostSpecificSubdivision();
            final com.maxmind.geoip2.record.City city = response.getCity();
            final Location location = response.getLocation();
            final Postal postal = response.getPostal();
            final com.maxmind.geoip2.record.Country registeredCountry = response.getRegisteredCountry();
            final Traits traits = response.getTraits();

            return new Result<>(
                new CacheableCityResponse(
                    isInEuropeanUnion(country),
                    country.getIsoCode(),
                    country.getName(),
                    continent.getCode(),
                    continent.getName(),
                    regionIsoCode(country, subdivision),
                    subdivision.getName(),
                    city.getName(),
                    location.getTimeZone(),
                    location.getLatitude(),
                    location.getLongitude(),
                    location.getAccuracyRadius(),
                    postal.getCode(),
                    isInEuropeanUnion(registeredCountry),
                    registeredCountry.getIsoCode(),
                    registeredCountry.getName()
                ),
                traits.getIpAddress(),
                traits.getNetwork().toString()
            );
        }

        @Override
        protected Map<String, Object> transform(final Result<CacheableCityResponse> result) {
            CacheableCityResponse response = result.result();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", result.ip());
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.isInEuropeanUnion != null) {
                            data.put("country_in_european_union", response.isInEuropeanUnion);
                        }
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.countryIsoCode != null) {
                            data.put("country_iso_code", response.countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        if (response.countryName != null) {
                            data.put("country_name", response.countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        if (response.continentCode != null) {
                            data.put("continent_code", response.continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        if (response.continentName != null) {
                            data.put("continent_name", response.continentName);
                        }
                    }
                    case REGION_ISO_CODE -> {
                        if (response.regionIsoCode != null) {
                            data.put("region_iso_code", response.regionIsoCode);
                        }
                    }
                    case REGION_NAME -> {
                        if (response.regionName != null) {
                            data.put("region_name", response.regionName);
                        }
                    }
                    case CITY_NAME -> {
                        if (response.cityName != null) {
                            data.put("city_name", response.cityName);
                        }
                    }
                    case TIMEZONE -> {
                        if (response.timezone != null) {
                            data.put("timezone", response.timezone);
                        }
                    }
                    case LOCATION -> {
                        Double latitude = response.latitude;
                        Double longitude = response.longitude;
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = HashMap.newHashMap(2);
                            locationObject.put("lat", latitude);
                            locationObject.put("lon", longitude);
                            data.put("location", locationObject);
                        }
                    }
                    case ACCURACY_RADIUS -> {
                        if (response.accuracyRadius != null) {
                            data.put("accuracy_radius", response.accuracyRadius);
                        }
                    }
                    case POSTAL_CODE -> {
                        if (response.postalCode != null) {
                            data.put("postal_code", response.postalCode);
                        }
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.registeredCountryIsInEuropeanUnion != null) {
                            data.put("registered_country_in_european_union", response.registeredCountryIsInEuropeanUnion);
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (response.registeredCountryIsoCode != null) {
                            data.put("registered_country_iso_code", response.registeredCountryIsoCode);
                        }
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (response.registeredCountryName != null) {
                            data.put("registered_country_name", response.registeredCountryName);
                        }
                    }
                }
            }
            return data;
        }
    }

    static class ConnectionType extends AbstractBase<ConnectionTypeResponse, ConnectionTypeResponse> {
        ConnectionType(final Set<Database.Property> properties) {
            super(
                properties,
                ConnectionTypeResponse.class,
                (response, ipAddress, network, locales) -> new ConnectionTypeResponse(response, ipAddress, network)
            );
        }

        @Override
        protected ConnectionTypeResponse cacheableRecord(ConnectionTypeResponse response) {
            return response;
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

    record CacheableCountryResponse(
        Boolean isInEuropeanUnion,
        String countryIsoCode,
        String countryName,
        String continentCode,
        String continentName,
        Boolean registeredCountryIsInEuropeanUnion,
        String registeredCountryIsoCode,
        String registeredCountryName
    ) {}

    static class Country extends AbstractBase<CountryResponse, Result<CacheableCountryResponse>> {
        Country(final Set<Database.Property> properties) {
            super(properties, CountryResponse.class, CountryResponse::new);
        }

        @Override
        protected Result<CacheableCountryResponse> cacheableRecord(CountryResponse response) {
            final com.maxmind.geoip2.record.Country country = response.getCountry();
            final Continent continent = response.getContinent();
            final com.maxmind.geoip2.record.Country registeredCountry = response.getRegisteredCountry();
            final Traits traits = response.getTraits();
            return new Result<>(
                new CacheableCountryResponse(
                    isInEuropeanUnion(country),
                    country.getIsoCode(),
                    country.getName(),
                    continent.getCode(),
                    continent.getName(),
                    isInEuropeanUnion(registeredCountry),
                    registeredCountry.getIsoCode(),
                    registeredCountry.getName()
                ),
                traits.getIpAddress(),
                traits.getNetwork().toString()
            );
        }

        @Override
        protected Map<String, Object> transform(final Result<CacheableCountryResponse> result) {
            CacheableCountryResponse response = result.result();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", result.ip());
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.isInEuropeanUnion != null) {
                            data.put("country_in_european_union", response.isInEuropeanUnion);
                        }
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.countryIsoCode != null) {
                            data.put("country_iso_code", response.countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        if (response.countryName != null) {
                            data.put("country_name", response.countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        if (response.continentCode != null) {
                            data.put("continent_code", response.continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        if (response.continentName != null) {
                            data.put("continent_name", response.continentName);
                        }
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.registeredCountryIsInEuropeanUnion != null) {
                            data.put("registered_country_in_european_union", response.registeredCountryIsInEuropeanUnion);
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (response.registeredCountryIsoCode != null) {
                            data.put("registered_country_iso_code", response.registeredCountryIsoCode);
                        }
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (response.registeredCountryName != null) {
                            data.put("registered_country_name", response.registeredCountryName);
                        }
                    }
                }
            }
            return data;
        }
    }

    static class Domain extends AbstractBase<DomainResponse, DomainResponse> {
        Domain(final Set<Database.Property> properties) {
            super(
                properties,
                DomainResponse.class,
                (response, ipAddress, network, locales) -> new DomainResponse(response, ipAddress, network)
            );
        }

        @Override
        protected DomainResponse cacheableRecord(DomainResponse response) {
            return response;
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

    record CacheableEnterpriseResponse(
        Integer countryConfidence,
        Boolean isInEuropeanUnion,
        String countryIsoCode,
        String countryName,
        String continentCode,
        String continentName,
        String regionIsoCode,
        String regionName,
        Integer cityConfidence,
        String cityName,
        String timezone,
        Double latitude,
        Double longitude,
        Integer accuracyRadius,
        String postalCode,
        Integer postalConfidence,
        Long asn,
        String organizationName,
        boolean isHostingProvider,
        boolean isTorExitNode,
        boolean isAnonymousVpn,
        boolean isAnonymous,
        boolean isPublicProxy,
        boolean isResidentialProxy,
        String domain,
        String isp,
        String ispOrganization,
        String mobileCountryCode,
        String mobileNetworkCode,
        String userType,
        String connectionType,
        Boolean registeredCountryIsInEuropeanUnion,
        String registeredCountryIsoCode,
        String registeredCountryName
    ) {}

    static class Enterprise extends AbstractBase<EnterpriseResponse, Result<CacheableEnterpriseResponse>> {
        Enterprise(final Set<Database.Property> properties) {
            super(properties, EnterpriseResponse.class, EnterpriseResponse::new);
        }

        @Override
        protected Result<CacheableEnterpriseResponse> cacheableRecord(EnterpriseResponse response) {
            final com.maxmind.geoip2.record.Country country = response.getCountry();
            final Continent continent = response.getContinent();
            final Subdivision subdivision = response.getMostSpecificSubdivision();
            final com.maxmind.geoip2.record.City city = response.getCity();
            final Location location = response.getLocation();
            final Postal postal = response.getPostal();
            final com.maxmind.geoip2.record.Country registeredCountry = response.getRegisteredCountry();
            final Traits traits = response.getTraits();

            return new Result<>(
                new CacheableEnterpriseResponse(
                    country.getConfidence(),
                    isInEuropeanUnion(country),
                    country.getIsoCode(),
                    country.getName(),
                    continent.getCode(),
                    continent.getName(),
                    regionIsoCode(country, subdivision),
                    subdivision.getName(),
                    city.getConfidence(),
                    city.getName(),
                    location.getTimeZone(),
                    location.getLatitude(),
                    location.getLongitude(),
                    location.getAccuracyRadius(),
                    postal.getCode(),
                    postal.getConfidence(),
                    traits.getAutonomousSystemNumber(),
                    traits.getAutonomousSystemOrganization(),
                    traits.isHostingProvider(),
                    traits.isTorExitNode(),
                    traits.isAnonymousVpn(),
                    traits.isAnonymous(),
                    traits.isPublicProxy(),
                    traits.isResidentialProxy(),
                    traits.getDomain(),
                    traits.getIsp(),
                    traits.getOrganization(),
                    traits.getMobileCountryCode(),
                    traits.getMobileNetworkCode(),
                    traits.getUserType(),
                    traits.getConnectionType() == null ? null : traits.getConnectionType().toString(),
                    isInEuropeanUnion(registeredCountry),
                    registeredCountry.getIsoCode(),
                    registeredCountry.getName()
                ),
                traits.getIpAddress(),
                traits.getNetwork().toString()
            );
        }

        @Override
        protected Map<String, Object> transform(final Result<CacheableEnterpriseResponse> result) {
            CacheableEnterpriseResponse response = result.result();

            Map<String, Object> data = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> data.put("ip", result.ip());
                    case COUNTRY_CONFIDENCE -> {
                        if (response.countryConfidence != null) {
                            data.put("country_confidence", response.countryConfidence);
                        }
                    }
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.isInEuropeanUnion != null) {
                            data.put("country_in_european_union", response.isInEuropeanUnion);
                        }
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.countryIsoCode != null) {
                            data.put("country_iso_code", response.countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        if (response.countryName != null) {
                            data.put("country_name", response.countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        if (response.continentCode != null) {
                            data.put("continent_code", response.continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        if (response.continentName != null) {
                            data.put("continent_name", response.continentName);
                        }
                    }
                    case REGION_ISO_CODE -> {
                        if (response.regionIsoCode != null) {
                            data.put("region_iso_code", response.regionIsoCode);
                        }
                    }
                    case REGION_NAME -> {
                        if (response.regionName != null) {
                            data.put("region_name", response.regionName);
                        }
                    }
                    case CITY_CONFIDENCE -> {
                        if (response.cityConfidence != null) {
                            data.put("city_confidence", response.cityConfidence);
                        }
                    }
                    case CITY_NAME -> {
                        if (response.cityName != null) {
                            data.put("city_name", response.cityName);
                        }
                    }
                    case TIMEZONE -> {
                        if (response.timezone != null) {
                            data.put("timezone", response.timezone);
                        }
                    }
                    case LOCATION -> {
                        Double latitude = response.latitude;
                        Double longitude = response.longitude;
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = HashMap.newHashMap(2);
                            locationObject.put("lat", latitude);
                            locationObject.put("lon", longitude);
                            data.put("location", locationObject);
                        }
                    }
                    case ACCURACY_RADIUS -> {
                        if (response.accuracyRadius != null) {
                            data.put("accuracy_radius", response.accuracyRadius);
                        }
                    }
                    case POSTAL_CODE -> {
                        if (response.postalCode != null) {
                            data.put("postal_code", response.postalCode);
                        }
                    }
                    case POSTAL_CONFIDENCE -> {
                        if (response.postalConfidence != null) {
                            data.put("postal_confidence", response.postalConfidence);
                        }
                    }
                    case ASN -> {
                        if (response.asn != null) {
                            data.put("asn", response.asn);
                        }
                    }
                    case ORGANIZATION_NAME -> {
                        if (response.organizationName != null) {
                            data.put("organization_name", response.organizationName);
                        }
                    }
                    case NETWORK -> {
                        if (result.network() != null) {
                            data.put("network", result.network());
                        }
                    }
                    case HOSTING_PROVIDER -> {
                        data.put("hosting_provider", response.isHostingProvider);
                    }
                    case TOR_EXIT_NODE -> {
                        data.put("tor_exit_node", response.isTorExitNode);
                    }
                    case ANONYMOUS_VPN -> {
                        data.put("anonymous_vpn", response.isAnonymousVpn);
                    }
                    case ANONYMOUS -> {
                        data.put("anonymous", response.isAnonymous);
                    }
                    case PUBLIC_PROXY -> {
                        data.put("public_proxy", response.isPublicProxy);
                    }
                    case RESIDENTIAL_PROXY -> {
                        data.put("residential_proxy", response.isResidentialProxy);
                    }
                    case DOMAIN -> {
                        if (response.domain != null) {
                            data.put("domain", response.domain);
                        }
                    }
                    case ISP -> {
                        if (response.isp != null) {
                            data.put("isp", response.isp);
                        }
                    }
                    case ISP_ORGANIZATION_NAME -> {
                        if (response.ispOrganization != null) {
                            data.put("isp_organization_name", response.ispOrganization);
                        }
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (response.mobileCountryCode != null) {
                            data.put("mobile_country_code", response.mobileCountryCode);
                        }
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (response.mobileNetworkCode != null) {
                            data.put("mobile_network_code", response.mobileNetworkCode);
                        }
                    }
                    case USER_TYPE -> {
                        if (response.userType != null) {
                            data.put("user_type", response.userType);
                        }
                    }
                    case CONNECTION_TYPE -> {
                        if (response.connectionType != null) {
                            data.put("connection_type", response.connectionType);
                        }
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.registeredCountryIsInEuropeanUnion != null) {
                            data.put("registered_country_in_european_union", response.registeredCountryIsInEuropeanUnion);
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (response.registeredCountryIsoCode != null) {
                            data.put("registered_country_iso_code", response.registeredCountryIsoCode);
                        }
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (response.registeredCountryName != null) {
                            data.put("registered_country_name", response.registeredCountryName);
                        }
                    }
                }
            }
            return data;
        }
    }

    static class Isp extends AbstractBase<IspResponse, IspResponse> {
        Isp(final Set<Database.Property> properties) {
            super(properties, IspResponse.class, (response, ipAddress, network, locales) -> new IspResponse(response, ipAddress, network));
        }

        @Override
        protected IspResponse cacheableRecord(IspResponse response) {
            return response;
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
     * As an internal detail, the {@code com.maxmind.geoip2.model} classes that are populated by
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
    private abstract static class AbstractBase<RESPONSE extends AbstractResponse, RECORD> implements IpDataLookup {

        protected final Set<Database.Property> properties;
        protected final Class<RESPONSE> clazz;
        // see the docstring on ResponseBuilder to understand why this isn't yet another abstract method on this class
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
            final RECORD response = ipDatabase.getResponse(ipAddress, this::lookup);
            return (response == null) ? Map.of() : transform(response);
        }

        @Nullable
        private RECORD lookup(final Reader reader, final String ipAddress) throws IOException {
            final InetAddress ip = InetAddresses.forString(ipAddress);
            final DatabaseRecord<RESPONSE> entry = reader.getRecord(ip, clazz);
            final RESPONSE data = entry.getData();
            return (data == null)
                ? null
                : cacheableRecord(builder.build(data, NetworkAddress.format(ip), entry.getNetwork(), List.of("en")));
        }

        /**
         * Given a fully-populated response object, create a record that is suitable for caching. If the fully-populated response object
         * itself is suitable for caching, then it is acceptable to simply return it.
         */
        protected abstract RECORD cacheableRecord(RESPONSE response);

        /**
         * Extract the configured properties from the retrieved response.
         *
         * @param response the non-null response that was retrieved
         * @return a mapping of properties for the ip from the response
         */
        protected abstract Map<String, Object> transform(RECORD response);
    }

    @Nullable
    private static Boolean isInEuropeanUnion(com.maxmind.geoip2.record.Country country) {
        // isInEuropeanUnion is a lowercase-b boolean so it cannot be null, but it really only makes sense for us to return a value
        // for this if there's actually a real country here, as opposed to an empty null-object country, so we check for an iso code first
        return (country.getIsoCode() == null) ? null : country.isInEuropeanUnion();
    }

    @Nullable
    private static String regionIsoCode(final com.maxmind.geoip2.record.Country country, final Subdivision subdivision) {
        // ISO 3166-2 code for country subdivisions, see https://www.iso.org/iso-3166-country-codes.html
        final String countryIso = country.getIsoCode();
        final String subdivisionIso = subdivision.getIsoCode();
        if (countryIso != null && subdivisionIso != null) {
            return countryIso + "-" + subdivisionIso;
        } else {
            return null;
        }
    }
}
