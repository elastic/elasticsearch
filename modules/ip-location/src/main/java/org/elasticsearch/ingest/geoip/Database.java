/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.iplocation.api.DatabaseProperty;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.iplocation.api.DatabaseProperty.ACCURACY_RADIUS;
import static org.elasticsearch.iplocation.api.DatabaseProperty.ANONYMOUS;
import static org.elasticsearch.iplocation.api.DatabaseProperty.ANONYMOUS_VPN;
import static org.elasticsearch.iplocation.api.DatabaseProperty.ASN;
import static org.elasticsearch.iplocation.api.DatabaseProperty.CITY_CONFIDENCE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.CITY_NAME;
import static org.elasticsearch.iplocation.api.DatabaseProperty.CONNECTION_TYPE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.CONTINENT_CODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.CONTINENT_NAME;
import static org.elasticsearch.iplocation.api.DatabaseProperty.COUNTRY_CONFIDENCE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.COUNTRY_IN_EUROPEAN_UNION;
import static org.elasticsearch.iplocation.api.DatabaseProperty.COUNTRY_ISO_CODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.COUNTRY_NAME;
import static org.elasticsearch.iplocation.api.DatabaseProperty.HOSTING;
import static org.elasticsearch.iplocation.api.DatabaseProperty.HOSTING_PROVIDER;
import static org.elasticsearch.iplocation.api.DatabaseProperty.IP;
import static org.elasticsearch.iplocation.api.DatabaseProperty.ISP;
import static org.elasticsearch.iplocation.api.DatabaseProperty.ISP_ORGANIZATION_NAME;
import static org.elasticsearch.iplocation.api.DatabaseProperty.LOCATION;
import static org.elasticsearch.iplocation.api.DatabaseProperty.MOBILE_COUNTRY_CODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.MOBILE_NETWORK_CODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.NETWORK;
import static org.elasticsearch.iplocation.api.DatabaseProperty.ORGANIZATION_NAME;
import static org.elasticsearch.iplocation.api.DatabaseProperty.POSTAL_CODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.POSTAL_CONFIDENCE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.PROXY;
import static org.elasticsearch.iplocation.api.DatabaseProperty.PUBLIC_PROXY;
import static org.elasticsearch.iplocation.api.DatabaseProperty.REGION_ISO_CODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.REGION_NAME;
import static org.elasticsearch.iplocation.api.DatabaseProperty.REGISTERED_COUNTRY_IN_EUROPEAN_UNION;
import static org.elasticsearch.iplocation.api.DatabaseProperty.REGISTERED_COUNTRY_ISO_CODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.REGISTERED_COUNTRY_NAME;
import static org.elasticsearch.iplocation.api.DatabaseProperty.RELAY;
import static org.elasticsearch.iplocation.api.DatabaseProperty.RESIDENTIAL_PROXY;
import static org.elasticsearch.iplocation.api.DatabaseProperty.SERVICE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.TIMEZONE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.TOR;
import static org.elasticsearch.iplocation.api.DatabaseProperty.TOR_EXIT_NODE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.TYPE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.USER_TYPE;
import static org.elasticsearch.iplocation.api.DatabaseProperty.VPN;

/**
 * A high-level representation of a kind of ip location database that is used for resolving geolocation information based on IP addresses.
 * <p>
 * A database has a set of properties that are valid to use with it (see {@link Database#properties()}),
 * as well as a list of default properties to use if no properties are specified (see {@link Database#defaultProperties()}).
 * <p>
 * Some database providers have similar concepts but might have slightly different properties associated with those types.
 * This can be accommodated, for example, by having a Foo value and a separate FooV2 value where the 'V' should be read as
 * 'variant' or 'variation'. A V-less Database type is inherently the first variant/variation (i.e. V1).
 */
enum Database {

    City(
        Set.of(
            IP,
            COUNTRY_IN_EUROPEAN_UNION,
            COUNTRY_ISO_CODE,
            CONTINENT_CODE,
            COUNTRY_NAME,
            CONTINENT_NAME,
            REGION_ISO_CODE,
            REGION_NAME,
            CITY_NAME,
            TIMEZONE,
            LOCATION,
            POSTAL_CODE,
            ACCURACY_RADIUS,
            REGISTERED_COUNTRY_IN_EUROPEAN_UNION,
            REGISTERED_COUNTRY_ISO_CODE,
            REGISTERED_COUNTRY_NAME
        ),
        Set.of(COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_NAME, REGION_ISO_CODE, REGION_NAME, CITY_NAME, LOCATION)
    ),
    Country(
        Set.of(
            IP,
            CONTINENT_CODE,
            CONTINENT_NAME,
            COUNTRY_NAME,
            COUNTRY_IN_EUROPEAN_UNION,
            COUNTRY_ISO_CODE,
            REGISTERED_COUNTRY_IN_EUROPEAN_UNION,
            REGISTERED_COUNTRY_ISO_CODE,
            REGISTERED_COUNTRY_NAME
        ),
        Set.of(CONTINENT_NAME, COUNTRY_NAME, COUNTRY_ISO_CODE)
    ),
    Asn(Set.of(IP, ASN, ORGANIZATION_NAME, NETWORK), Set.of(IP, ASN, ORGANIZATION_NAME, NETWORK)),
    AnonymousIp(
        Set.of(IP, HOSTING_PROVIDER, TOR_EXIT_NODE, ANONYMOUS_VPN, ANONYMOUS, PUBLIC_PROXY, RESIDENTIAL_PROXY),
        Set.of(HOSTING_PROVIDER, TOR_EXIT_NODE, ANONYMOUS_VPN, ANONYMOUS, PUBLIC_PROXY, RESIDENTIAL_PROXY)
    ),
    ConnectionType(Set.of(IP, CONNECTION_TYPE), Set.of(CONNECTION_TYPE)),
    Domain(Set.of(IP, DatabaseProperty.DOMAIN), Set.of(DatabaseProperty.DOMAIN)),
    Enterprise(
        Set.of(
            IP,
            COUNTRY_CONFIDENCE,
            COUNTRY_IN_EUROPEAN_UNION,
            COUNTRY_ISO_CODE,
            COUNTRY_NAME,
            CONTINENT_CODE,
            CONTINENT_NAME,
            REGION_ISO_CODE,
            REGION_NAME,
            CITY_CONFIDENCE,
            CITY_NAME,
            TIMEZONE,
            LOCATION,
            ASN,
            ORGANIZATION_NAME,
            NETWORK,
            HOSTING_PROVIDER,
            TOR_EXIT_NODE,
            ANONYMOUS_VPN,
            ANONYMOUS,
            PUBLIC_PROXY,
            RESIDENTIAL_PROXY,
            DatabaseProperty.DOMAIN,
            ISP,
            ISP_ORGANIZATION_NAME,
            MOBILE_COUNTRY_CODE,
            MOBILE_NETWORK_CODE,
            USER_TYPE,
            CONNECTION_TYPE,
            POSTAL_CODE,
            POSTAL_CONFIDENCE,
            ACCURACY_RADIUS,
            REGISTERED_COUNTRY_IN_EUROPEAN_UNION,
            REGISTERED_COUNTRY_ISO_CODE,
            REGISTERED_COUNTRY_NAME
        ),
        Set.of(COUNTRY_ISO_CODE, COUNTRY_NAME, CONTINENT_NAME, REGION_ISO_CODE, REGION_NAME, CITY_NAME, LOCATION)
    ),
    Isp(
        Set.of(IP, ASN, ORGANIZATION_NAME, NETWORK, ISP, ISP_ORGANIZATION_NAME, MOBILE_COUNTRY_CODE, MOBILE_NETWORK_CODE),
        Set.of(IP, ASN, ORGANIZATION_NAME, NETWORK, ISP, ISP_ORGANIZATION_NAME, MOBILE_COUNTRY_CODE, MOBILE_NETWORK_CODE)
    ),
    AsnV2(
        Set.of(IP, ASN, ORGANIZATION_NAME, NETWORK, DatabaseProperty.DOMAIN, COUNTRY_ISO_CODE, TYPE),
        Set.of(IP, ASN, ORGANIZATION_NAME, NETWORK)
    ),
    CityV2(
        Set.of(IP, COUNTRY_ISO_CODE, REGION_NAME, CITY_NAME, TIMEZONE, LOCATION, POSTAL_CODE),
        Set.of(COUNTRY_ISO_CODE, REGION_NAME, CITY_NAME, LOCATION)
    ),
    CountryV2(
        Set.of(IP, CONTINENT_CODE, CONTINENT_NAME, COUNTRY_NAME, COUNTRY_ISO_CODE),
        Set.of(CONTINENT_NAME, COUNTRY_NAME, COUNTRY_ISO_CODE)
    ),
    PrivacyDetection(Set.of(IP, HOSTING, PROXY, RELAY, TOR, VPN, SERVICE), Set.of(HOSTING, PROXY, RELAY, TOR, VPN, SERVICE));

    private final Set<DatabaseProperty> properties;
    private final Set<DatabaseProperty> defaultProperties;

    Database(Set<DatabaseProperty> properties, Set<DatabaseProperty> defaultProperties) {
        this.properties = properties;
        this.defaultProperties = defaultProperties;
    }

    /**
     * @return a set representing all the valid properties for this database
     */
    public Set<DatabaseProperty> properties() {
        return properties;
    }

    /**
     * @return a set representing the default properties for this database
     */
    public Set<DatabaseProperty> defaultProperties() {
        return defaultProperties;
    }

    /**
     * Parse the given list of property names.
     *
     * @param propertyNames a list of property names to parse, or null to use the default properties for this database
     * @throws IllegalArgumentException if any of the property names are not valid
     * @return a set of parsed and validated properties
     */
    public Set<DatabaseProperty> parseProperties(@Nullable final List<String> propertyNames) {
        if (propertyNames != null) {
            final Set<DatabaseProperty> parsedProperties = new HashSet<>();
            for (String propertyName : propertyNames) {
                parsedProperties.add(DatabaseProperty.parseProperty(this.properties, propertyName));
            }
            return Set.copyOf(parsedProperties);
        } else {
            return this.defaultProperties;
        }
    }
}
