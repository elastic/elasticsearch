/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.Network;
import com.maxmind.geoip2.DatabaseReader;
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
import com.maxmind.geoip2.record.Subdivision;

import org.elasticsearch.common.network.NetworkAddress;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A collection of {@link GeoDataLookup} implementations for Maxmind databases
 */
class MaxmindGeoDataLookups {
    private MaxmindGeoDataLookups() {}

    static class AnonymousIp extends AbstractBase<AnonymousIpResponse> {
        AnonymousIp(final Set<Database.Property> properties) {
            super(DatabaseReader::tryAnonymousIp, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final AnonymousIpResponse response) {
            if (response == null) {
                return Map.of();
            }

            boolean isHostingProvider = response.isHostingProvider();
            boolean isTorExitNode = response.isTorExitNode();
            boolean isAnonymousVpn = response.isAnonymousVpn();
            boolean isAnonymous = response.isAnonymous();
            boolean isPublicProxy = response.isPublicProxy();
            boolean isResidentialProxy = response.isResidentialProxy();

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case HOSTING_PROVIDER -> {
                        geoData.put("hosting_provider", isHostingProvider);
                    }
                    case TOR_EXIT_NODE -> {
                        geoData.put("tor_exit_node", isTorExitNode);
                    }
                    case ANONYMOUS_VPN -> {
                        geoData.put("anonymous_vpn", isAnonymousVpn);
                    }
                    case ANONYMOUS -> {
                        geoData.put("anonymous", isAnonymous);
                    }
                    case PUBLIC_PROXY -> {
                        geoData.put("public_proxy", isPublicProxy);
                    }
                    case RESIDENTIAL_PROXY -> {
                        geoData.put("residential_proxy", isResidentialProxy);
                    }
                }
            }
            return geoData;
        }
    }

    static class Asn extends AbstractBase<AsnResponse> {
        Asn(Set<Database.Property> properties) {
            super(DatabaseReader::tryAsn, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final AsnResponse response) {
            if (response == null) {
                return Map.of();
            }
            Long asn = response.getAutonomousSystemNumber();
            String organizationName = response.getAutonomousSystemOrganization();
            Network network = response.getNetwork();

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case ASN -> {
                        if (asn != null) {
                            geoData.put("asn", asn);
                        }
                    }
                    case ORGANIZATION_NAME -> {
                        if (organizationName != null) {
                            geoData.put("organization_name", organizationName);
                        }
                    }
                    case NETWORK -> {
                        if (network != null) {
                            geoData.put("network", network.toString());
                        }
                    }
                }
            }
            return geoData;
        }
    }

    static class City extends AbstractBase<CityResponse> {
        City(final Set<Database.Property> properties) {
            super(DatabaseReader::tryCity, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final CityResponse response) {
            if (response == null) {
                return Map.of();
            }
            com.maxmind.geoip2.record.Country country = response.getCountry();
            com.maxmind.geoip2.record.City city = response.getCity();
            Location location = response.getLocation();
            Continent continent = response.getContinent();
            Subdivision subdivision = response.getMostSpecificSubdivision();

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case COUNTRY_ISO_CODE -> {
                        String countryIsoCode = country.getIsoCode();
                        if (countryIsoCode != null) {
                            geoData.put("country_iso_code", countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        String countryName = country.getName();
                        if (countryName != null) {
                            geoData.put("country_name", countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        String continentCode = continent.getCode();
                        if (continentCode != null) {
                            geoData.put("continent_code", continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        String continentName = continent.getName();
                        if (continentName != null) {
                            geoData.put("continent_name", continentName);
                        }
                    }
                    case REGION_ISO_CODE -> {
                        // ISO 3166-2 code for country subdivisions.
                        // See iso.org/iso-3166-country-codes.html
                        String countryIso = country.getIsoCode();
                        String subdivisionIso = subdivision.getIsoCode();
                        if (countryIso != null && subdivisionIso != null) {
                            String regionIsoCode = countryIso + "-" + subdivisionIso;
                            geoData.put("region_iso_code", regionIsoCode);
                        }
                    }
                    case REGION_NAME -> {
                        String subdivisionName = subdivision.getName();
                        if (subdivisionName != null) {
                            geoData.put("region_name", subdivisionName);
                        }
                    }
                    case CITY_NAME -> {
                        String cityName = city.getName();
                        if (cityName != null) {
                            geoData.put("city_name", cityName);
                        }
                    }
                    case TIMEZONE -> {
                        String locationTimeZone = location.getTimeZone();
                        if (locationTimeZone != null) {
                            geoData.put("timezone", locationTimeZone);
                        }
                    }
                    case LOCATION -> {
                        Double latitude = location.getLatitude();
                        Double longitude = location.getLongitude();
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = new HashMap<>();
                            locationObject.put("lat", latitude);
                            locationObject.put("lon", longitude);
                            geoData.put("location", locationObject);
                        }
                    }
                }
            }
            return geoData;
        }
    }

    static class ConnectionType extends AbstractBase<ConnectionTypeResponse> {
        ConnectionType(final Set<Database.Property> properties) {
            super(DatabaseReader::tryConnectionType, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final ConnectionTypeResponse response) {
            if (response == null) {
                return Map.of();
            }

            ConnectionTypeResponse.ConnectionType connectionType = response.getConnectionType();

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case CONNECTION_TYPE -> {
                        if (connectionType != null) {
                            geoData.put("connection_type", connectionType.toString());
                        }
                    }
                }
            }
            return geoData;
        }
    }

    static class Country extends AbstractBase<CountryResponse> {
        Country(final Set<Database.Property> properties) {
            super(DatabaseReader::tryCountry, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final CountryResponse response) {
            if (response == null) {
                return Map.of();
            }
            com.maxmind.geoip2.record.Country country = response.getCountry();
            Continent continent = response.getContinent();

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case COUNTRY_ISO_CODE -> {
                        String countryIsoCode = country.getIsoCode();
                        if (countryIsoCode != null) {
                            geoData.put("country_iso_code", countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        String countryName = country.getName();
                        if (countryName != null) {
                            geoData.put("country_name", countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        String continentCode = continent.getCode();
                        if (continentCode != null) {
                            geoData.put("continent_code", continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        String continentName = continent.getName();
                        if (continentName != null) {
                            geoData.put("continent_name", continentName);
                        }
                    }
                }
            }
            return geoData;
        }
    }

    static class Domain extends AbstractBase<DomainResponse> {
        Domain(final Set<Database.Property> properties) {
            super(DatabaseReader::tryDomain, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final DomainResponse response) {
            if (response == null) {
                return Map.of();
            }

            String domain = response.getDomain();

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case DOMAIN -> {
                        if (domain != null) {
                            geoData.put("domain", domain);
                        }
                    }
                }
            }
            return geoData;
        }
    }

    static class Enterprise extends AbstractBase<EnterpriseResponse> {
        Enterprise(final Set<Database.Property> properties) {
            super(DatabaseReader::tryEnterprise, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final EnterpriseResponse response) {
            if (response == null) {
                return Map.of();
            }

            com.maxmind.geoip2.record.Country country = response.getCountry();
            com.maxmind.geoip2.record.City city = response.getCity();
            Location location = response.getLocation();
            Continent continent = response.getContinent();
            Subdivision subdivision = response.getMostSpecificSubdivision();

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

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case COUNTRY_ISO_CODE -> {
                        String countryIsoCode = country.getIsoCode();
                        if (countryIsoCode != null) {
                            geoData.put("country_iso_code", countryIsoCode);
                        }
                    }
                    case COUNTRY_NAME -> {
                        String countryName = country.getName();
                        if (countryName != null) {
                            geoData.put("country_name", countryName);
                        }
                    }
                    case CONTINENT_CODE -> {
                        String continentCode = continent.getCode();
                        if (continentCode != null) {
                            geoData.put("continent_code", continentCode);
                        }
                    }
                    case CONTINENT_NAME -> {
                        String continentName = continent.getName();
                        if (continentName != null) {
                            geoData.put("continent_name", continentName);
                        }
                    }
                    case REGION_ISO_CODE -> {
                        // ISO 3166-2 code for country subdivisions.
                        // See iso.org/iso-3166-country-codes.html
                        String countryIso = country.getIsoCode();
                        String subdivisionIso = subdivision.getIsoCode();
                        if (countryIso != null && subdivisionIso != null) {
                            String regionIsoCode = countryIso + "-" + subdivisionIso;
                            geoData.put("region_iso_code", regionIsoCode);
                        }
                    }
                    case REGION_NAME -> {
                        String subdivisionName = subdivision.getName();
                        if (subdivisionName != null) {
                            geoData.put("region_name", subdivisionName);
                        }
                    }
                    case CITY_NAME -> {
                        String cityName = city.getName();
                        if (cityName != null) {
                            geoData.put("city_name", cityName);
                        }
                    }
                    case TIMEZONE -> {
                        String locationTimeZone = location.getTimeZone();
                        if (locationTimeZone != null) {
                            geoData.put("timezone", locationTimeZone);
                        }
                    }
                    case LOCATION -> {
                        Double latitude = location.getLatitude();
                        Double longitude = location.getLongitude();
                        if (latitude != null && longitude != null) {
                            Map<String, Object> locationObject = new HashMap<>();
                            locationObject.put("lat", latitude);
                            locationObject.put("lon", longitude);
                            geoData.put("location", locationObject);
                        }
                    }
                    case ASN -> {
                        if (asn != null) {
                            geoData.put("asn", asn);
                        }
                    }
                    case ORGANIZATION_NAME -> {
                        if (organizationName != null) {
                            geoData.put("organization_name", organizationName);
                        }
                    }
                    case NETWORK -> {
                        if (network != null) {
                            geoData.put("network", network.toString());
                        }
                    }
                    case HOSTING_PROVIDER -> {
                        geoData.put("hosting_provider", isHostingProvider);
                    }
                    case TOR_EXIT_NODE -> {
                        geoData.put("tor_exit_node", isTorExitNode);
                    }
                    case ANONYMOUS_VPN -> {
                        geoData.put("anonymous_vpn", isAnonymousVpn);
                    }
                    case ANONYMOUS -> {
                        geoData.put("anonymous", isAnonymous);
                    }
                    case PUBLIC_PROXY -> {
                        geoData.put("public_proxy", isPublicProxy);
                    }
                    case RESIDENTIAL_PROXY -> {
                        geoData.put("residential_proxy", isResidentialProxy);
                    }
                    case DOMAIN -> {
                        if (domain != null) {
                            geoData.put("domain", domain);
                        }
                    }
                    case ISP -> {
                        if (isp != null) {
                            geoData.put("isp", isp);
                        }
                    }
                    case ISP_ORGANIZATION_NAME -> {
                        if (ispOrganization != null) {
                            geoData.put("isp_organization", ispOrganization);
                        }
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (mobileCountryCode != null) {
                            geoData.put("mobile_country_code", mobileCountryCode);
                        }
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (mobileNetworkCode != null) {
                            geoData.put("mobile_network_code", mobileNetworkCode);
                        }
                    }
                    case USER_TYPE -> {
                        if (userType != null) {
                            geoData.put("user_type", userType);
                        }
                    }
                    case CONNECTION_TYPE -> {
                        if (connectionType != null) {
                            geoData.put("connection_type", connectionType.toString());
                        }
                    }
                }
            }
            return geoData;
        }
    }

    static class Isp extends AbstractBase<IspResponse> {
        Isp(final Set<Database.Property> properties) {
            super(DatabaseReader::tryIsp, properties);
        }

        @Override
        protected Map<String, Object> transformResponse(final InetAddress ipAddress, final IspResponse response) {
            if (response == null) {
                return Map.of();
            }

            String isp = response.getIsp();
            String ispOrganization = response.getOrganization();
            String mobileNetworkCode = response.getMobileNetworkCode();
            String mobileCountryCode = response.getMobileCountryCode();
            Long asn = response.getAutonomousSystemNumber();
            String organizationName = response.getAutonomousSystemOrganization();
            Network network = response.getNetwork();

            Map<String, Object> geoData = new HashMap<>();
            for (Database.Property property : this.properties) {
                switch (property) {
                    case IP -> geoData.put("ip", NetworkAddress.format(ipAddress));
                    case ASN -> {
                        if (asn != null) {
                            geoData.put("asn", asn);
                        }
                    }
                    case ORGANIZATION_NAME -> {
                        if (organizationName != null) {
                            geoData.put("organization_name", organizationName);
                        }
                    }
                    case NETWORK -> {
                        if (network != null) {
                            geoData.put("network", network.toString());
                        }
                    }
                    case ISP -> {
                        if (isp != null) {
                            geoData.put("isp", isp);
                        }
                    }
                    case ISP_ORGANIZATION_NAME -> {
                        if (ispOrganization != null) {
                            geoData.put("isp_organization", ispOrganization);
                        }
                    }
                    case MOBILE_COUNTRY_CODE -> {
                        if (mobileCountryCode != null) {
                            geoData.put("mobile_country_code", mobileCountryCode);
                        }
                    }
                    case MOBILE_NETWORK_CODE -> {
                        if (mobileNetworkCode != null) {
                            geoData.put("mobile_network_code", mobileNetworkCode);
                        }
                    }
                }
            }
            return geoData;
        }
    }

    /**
     * The {@code MaxmindGeoDataLookups.AbstractBase} is an abstract base implementation of {@link GeoDataLookup} that
     * provides common functionality for getting a specific kind of {@link AbstractResponse} from a {@link GeoIpDatabase}.
     *
     * @param <RESPONSE> the intermediate type of {@link AbstractResponse}
     */
    private abstract static class AbstractBase<RESPONSE extends AbstractResponse> implements GeoDataLookup {

        protected final Set<Database.Property> properties;
        private final DatabaseReaderResponseLookup<RESPONSE> databaseReaderResponseLookup;

        AbstractBase(final DatabaseReaderResponseLookup<RESPONSE> databaseReaderResponseLookup, final Set<Database.Property> properties) {
            this.databaseReaderResponseLookup = databaseReaderResponseLookup;
            this.properties = Set.copyOf(properties);
        }

        @Override
        public Set<Database.Property> getProperties() {
            return this.properties;
        }

        @Override
        public final Map<String, Object> getGeoData(final GeoIpDatabase geoIpDatabase, final InetAddress ipAddress) throws IOException {
            return transformResponse(ipAddress, geoIpDatabase.getResponse(ipAddress, databaseReaderResponseLookup::apply));
        }

        /**
         * Extract the configured properties from the retrieved response
         * @param ipAddress the ip address that was used to retrieve the response
         * @param response the response that was retrieved
         * @return a mapping of properties for the ip from the response
         */
        protected abstract Map<String, Object> transformResponse(InetAddress ipAddress, RESPONSE response);

        /**
         * @apiNote typically a method reference like {@code DatabaseReader::tryCity}
         * @param <RESPONSE> the type of response to lookup
         */
        @FunctionalInterface
        interface DatabaseReaderResponseLookup<RESPONSE extends AbstractResponse> {
            Optional<RESPONSE> apply(DatabaseReader databaseReader, InetAddress ipAddress) throws Exception;
        }
    }
}
