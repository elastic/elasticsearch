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
import org.elasticsearch.ingest.geoip.InternalIpDataLookup.Result;
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * A collection of {@link InternalIpDataLookup} implementations for MaxMind databases
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
    static Function<Set<DatabaseProperty>, InternalIpDataLookup> getMaxmindLookup(final Database database) {
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
        AnonymousIp(final Set<DatabaseProperty> properties) {
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
        protected void transform(final AnonymousIpResponse response, final IpLocationInfoCollector collector) {
            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(response.getIpAddress());
                    case HOSTING_PROVIDER -> collector.hostingProvider(response.isHostingProvider());
                    case TOR_EXIT_NODE -> collector.torExitNode(response.isTorExitNode());
                    case ANONYMOUS_VPN -> collector.anonymousVpn(response.isAnonymousVpn());
                    case ANONYMOUS -> collector.anonymous(response.isAnonymous());
                    case PUBLIC_PROXY -> collector.publicProxy(response.isPublicProxy());
                    case RESIDENTIAL_PROXY -> collector.residentialProxy(response.isResidentialProxy());
                }
            }
        }
    }

    static class Asn extends AbstractBase<AsnResponse, AsnResponse> {
        Asn(Set<DatabaseProperty> properties) {
            super(properties, AsnResponse.class, (response, ipAddress, network, locales) -> new AsnResponse(response, ipAddress, network));
        }

        @Override
        protected AsnResponse cacheableRecord(AsnResponse response) {
            return response;
        }

        @Override
        protected void transform(final AsnResponse response, final IpLocationInfoCollector collector) {
            Long asn = response.getAutonomousSystemNumber();
            String organizationName = response.getAutonomousSystemOrganization();
            Network network = response.getNetwork();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(response.getIpAddress());
                    case ASN -> {
                        if (asn != null) collector.asn(asn);
                    }
                    case ORGANIZATION_NAME -> {
                        if (organizationName != null) collector.organizationName(organizationName);
                    }
                    case NETWORK -> {
                        if (network != null) collector.network(network.toString());
                    }
                }
            }
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
        City(final Set<DatabaseProperty> properties) {
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
        protected void transform(final Result<CacheableCityResponse> result, final IpLocationInfoCollector collector) {
            CacheableCityResponse response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.isInEuropeanUnion != null) collector.countryInEuropeanUnion(response.isInEuropeanUnion);
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.countryIsoCode != null) collector.countryIsoCode(response.countryIsoCode);
                    }
                    case COUNTRY_NAME -> {
                        if (response.countryName != null) collector.countryName(response.countryName);
                    }
                    case CONTINENT_CODE -> {
                        if (response.continentCode != null) collector.continentCode(response.continentCode);
                    }
                    case CONTINENT_NAME -> {
                        if (response.continentName != null) collector.continentName(response.continentName);
                    }
                    case REGION_ISO_CODE -> {
                        if (response.regionIsoCode != null) collector.regionIsoCode(response.regionIsoCode);
                    }
                    case REGION_NAME -> {
                        if (response.regionName != null) collector.regionName(response.regionName);
                    }
                    case CITY_NAME -> {
                        if (response.cityName != null) collector.cityName(response.cityName);
                    }
                    case TIMEZONE -> {
                        if (response.timezone != null) collector.timezone(response.timezone);
                    }
                    case LOCATION -> {
                        if (response.latitude != null && response.longitude != null) {
                            collector.location(response.latitude, response.longitude);
                        }
                    }
                    case ACCURACY_RADIUS -> {
                        if (response.accuracyRadius != null) collector.accuracyRadius(response.accuracyRadius);
                    }
                    case POSTAL_CODE -> {
                        if (response.postalCode != null) collector.postalCode(response.postalCode);
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.registeredCountryIsInEuropeanUnion != null) {
                            collector.registeredCountryInEuropeanUnion(response.registeredCountryIsInEuropeanUnion);
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (response.registeredCountryIsoCode != null) collector.registeredCountryIsoCode(
                            response.registeredCountryIsoCode
                        );
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (response.registeredCountryName != null) collector.registeredCountryName(response.registeredCountryName);
                    }
                }
            }
        }
    }

    static class ConnectionType extends AbstractBase<ConnectionTypeResponse, ConnectionTypeResponse> {
        ConnectionType(final Set<DatabaseProperty> properties) {
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
        protected void transform(final ConnectionTypeResponse response, final IpLocationInfoCollector collector) {
            ConnectionTypeResponse.ConnectionType connectionType = response.getConnectionType();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(response.getIpAddress());
                    case CONNECTION_TYPE -> {
                        if (connectionType != null) collector.connectionType(connectionType.toString());
                    }
                }
            }
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
        Country(final Set<DatabaseProperty> properties) {
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
        protected void transform(final Result<CacheableCountryResponse> result, final IpLocationInfoCollector collector) {
            CacheableCountryResponse response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.isInEuropeanUnion != null) collector.countryInEuropeanUnion(response.isInEuropeanUnion);
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.countryIsoCode != null) collector.countryIsoCode(response.countryIsoCode);
                    }
                    case COUNTRY_NAME -> {
                        if (response.countryName != null) collector.countryName(response.countryName);
                    }
                    case CONTINENT_CODE -> {
                        if (response.continentCode != null) collector.continentCode(response.continentCode);
                    }
                    case CONTINENT_NAME -> {
                        if (response.continentName != null) collector.continentName(response.continentName);
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.registeredCountryIsInEuropeanUnion != null) {
                            collector.registeredCountryInEuropeanUnion(response.registeredCountryIsInEuropeanUnion);
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (response.registeredCountryIsoCode != null) collector.registeredCountryIsoCode(
                            response.registeredCountryIsoCode
                        );
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (response.registeredCountryName != null) collector.registeredCountryName(response.registeredCountryName);
                    }
                }
            }
        }
    }

    static class Domain extends AbstractBase<DomainResponse, DomainResponse> {
        Domain(final Set<DatabaseProperty> properties) {
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
        protected void transform(final DomainResponse response, final IpLocationInfoCollector collector) {
            String domain = response.getDomain();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(response.getIpAddress());
                    case DOMAIN -> {
                        if (domain != null) collector.domain(domain);
                    }
                }
            }
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
        Enterprise(final Set<DatabaseProperty> properties) {
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
        protected void transform(final Result<CacheableEnterpriseResponse> result, final IpLocationInfoCollector collector) {
            CacheableEnterpriseResponse response = result.result();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(result.ip());
                    case COUNTRY_CONFIDENCE -> {
                        if (response.countryConfidence != null) collector.countryConfidence(response.countryConfidence);
                    }
                    case COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.isInEuropeanUnion != null) collector.countryInEuropeanUnion(response.isInEuropeanUnion);
                    }
                    case COUNTRY_ISO_CODE -> {
                        if (response.countryIsoCode != null) collector.countryIsoCode(response.countryIsoCode);
                    }
                    case COUNTRY_NAME -> {
                        if (response.countryName != null) collector.countryName(response.countryName);
                    }
                    case CONTINENT_CODE -> {
                        if (response.continentCode != null) collector.continentCode(response.continentCode);
                    }
                    case CONTINENT_NAME -> {
                        if (response.continentName != null) collector.continentName(response.continentName);
                    }
                    case REGION_ISO_CODE -> {
                        if (response.regionIsoCode != null) collector.regionIsoCode(response.regionIsoCode);
                    }
                    case REGION_NAME -> {
                        if (response.regionName != null) collector.regionName(response.regionName);
                    }
                    case CITY_CONFIDENCE -> {
                        if (response.cityConfidence != null) collector.cityConfidence(response.cityConfidence);
                    }
                    case CITY_NAME -> {
                        if (response.cityName != null) collector.cityName(response.cityName);
                    }
                    case TIMEZONE -> {
                        if (response.timezone != null) collector.timezone(response.timezone);
                    }
                    case LOCATION -> {
                        if (response.latitude != null && response.longitude != null) {
                            collector.location(response.latitude, response.longitude);
                        }
                    }
                    case ACCURACY_RADIUS -> {
                        if (response.accuracyRadius != null) collector.accuracyRadius(response.accuracyRadius);
                    }
                    case POSTAL_CODE -> {
                        if (response.postalCode != null) collector.postalCode(response.postalCode);
                    }
                    case POSTAL_CONFIDENCE -> {
                        if (response.postalConfidence != null) collector.postalConfidence(response.postalConfidence);
                    }
                    case ASN -> {
                        if (response.asn != null) collector.asn(response.asn);
                    }
                    case ORGANIZATION_NAME -> {
                        if (response.organizationName != null) collector.organizationName(response.organizationName);
                    }
                    case NETWORK -> {
                        if (result.network() != null) collector.network(result.network());
                    }
                    case HOSTING_PROVIDER -> collector.hostingProvider(response.isHostingProvider);
                    case TOR_EXIT_NODE -> collector.torExitNode(response.isTorExitNode);
                    case ANONYMOUS_VPN -> collector.anonymousVpn(response.isAnonymousVpn);
                    case ANONYMOUS -> collector.anonymous(response.isAnonymous);
                    case PUBLIC_PROXY -> collector.publicProxy(response.isPublicProxy);
                    case RESIDENTIAL_PROXY -> collector.residentialProxy(response.isResidentialProxy);
                    case DOMAIN -> {
                        if (response.domain != null) collector.domain(response.domain);
                    }
                    case ISP -> {
                        if (response.isp != null) collector.isp(response.isp);
                    }
                    case ISP_ORGANIZATION_NAME -> {
                        if (response.ispOrganization != null) collector.ispOrganizationName(response.ispOrganization);
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (response.mobileCountryCode != null) collector.mobileCountryCode(response.mobileCountryCode);
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (response.mobileNetworkCode != null) collector.mobileNetworkCode(response.mobileNetworkCode);
                    }
                    case USER_TYPE -> {
                        if (response.userType != null) collector.userType(response.userType);
                    }
                    case CONNECTION_TYPE -> {
                        if (response.connectionType != null) collector.connectionType(response.connectionType);
                    }
                    case REGISTERED_COUNTRY_IN_EUROPEAN_UNION -> {
                        if (response.registeredCountryIsInEuropeanUnion != null) {
                            collector.registeredCountryInEuropeanUnion(response.registeredCountryIsInEuropeanUnion);
                        }
                    }
                    case REGISTERED_COUNTRY_ISO_CODE -> {
                        if (response.registeredCountryIsoCode != null) collector.registeredCountryIsoCode(
                            response.registeredCountryIsoCode
                        );
                    }
                    case REGISTERED_COUNTRY_NAME -> {
                        if (response.registeredCountryName != null) collector.registeredCountryName(response.registeredCountryName);
                    }
                }
            }
        }
    }

    static class Isp extends AbstractBase<IspResponse, IspResponse> {
        Isp(final Set<DatabaseProperty> properties) {
            super(properties, IspResponse.class, (response, ipAddress, network, locales) -> new IspResponse(response, ipAddress, network));
        }

        @Override
        protected IspResponse cacheableRecord(IspResponse response) {
            return response;
        }

        @Override
        protected void transform(final IspResponse response, final IpLocationInfoCollector collector) {
            String isp = response.getIsp();
            String ispOrganization = response.getOrganization();
            String mobileNetworkCode = response.getMobileNetworkCode();
            String mobileCountryCode = response.getMobileCountryCode();
            Long asn = response.getAutonomousSystemNumber();
            String organizationName = response.getAutonomousSystemOrganization();
            Network network = response.getNetwork();

            for (DatabaseProperty property : this.properties) {
                switch (property) {
                    case IP -> collector.ip(response.getIpAddress());
                    case ASN -> {
                        if (asn != null) collector.asn(asn);
                    }
                    case ORGANIZATION_NAME -> {
                        if (organizationName != null) collector.organizationName(organizationName);
                    }
                    case NETWORK -> {
                        if (network != null) collector.network(network.toString());
                    }
                    case ISP -> {
                        if (isp != null) collector.isp(isp);
                    }
                    case ISP_ORGANIZATION_NAME -> {
                        if (ispOrganization != null) collector.ispOrganizationName(ispOrganization);
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (mobileCountryCode != null) collector.mobileCountryCode(mobileCountryCode);
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (mobileNetworkCode != null) collector.mobileNetworkCode(mobileNetworkCode);
                    }
                }
            }
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
     * The {@link MaxmindIpDataLookups.AbstractBase} is an abstract base implementation of {@link InternalIpDataLookup} that
     * provides common functionality for getting a specific kind of {@link AbstractResponse} from a {@link IpDatabase}.
     *
     * @param <RESPONSE> the intermediate type of {@link AbstractResponse}
     */
    private abstract static class AbstractBase<RESPONSE extends AbstractResponse, RECORD> implements InternalIpDataLookup {

        protected final Set<DatabaseProperty> properties;
        protected final Class<RESPONSE> clazz;
        // see the docstring on ResponseBuilder to understand why this isn't yet another abstract method on this class
        protected final ResponseBuilder<RESPONSE> builder;

        AbstractBase(final Set<DatabaseProperty> properties, final Class<RESPONSE> clazz, final ResponseBuilder<RESPONSE> builder) {
            this.properties = Set.copyOf(properties);
            this.clazz = clazz;
            this.builder = builder;
        }

        @Override
        public Set<DatabaseProperty> getProperties() {
            return this.properties;
        }

        @Override
        public final boolean getData(final IpDatabase ipDatabase, final String ipAddress, final IpLocationInfoCollector collector) {
            final RECORD response = ipDatabase.getResponse(ipAddress, this::lookup);
            if (response == null) {
                return false;
            }
            transform(response, collector);
            return true;
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
         * Extract the configured properties from the retrieved response and push them to the collector.
         *
         * @param response the non-null response that was retrieved
         * @param collector the collector to push results to
         */
        protected abstract void transform(RECORD response, IpLocationInfoCollector collector);
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
