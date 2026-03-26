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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

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
            Property.IP,
            Property.COUNTRY_IN_EUROPEAN_UNION,
            Property.COUNTRY_ISO_CODE,
            Property.CONTINENT_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.TIMEZONE,
            Property.LOCATION,
            Property.POSTAL_CODE,
            Property.ACCURACY_RADIUS,
            Property.REGISTERED_COUNTRY_IN_EUROPEAN_UNION,
            Property.REGISTERED_COUNTRY_ISO_CODE,
            Property.REGISTERED_COUNTRY_NAME
        ),
        Set.of(
            Property.COUNTRY_ISO_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.LOCATION
        )
    ),
    Country(
        Set.of(
            Property.IP,
            Property.CONTINENT_CODE,
            Property.CONTINENT_NAME,
            Property.COUNTRY_NAME,
            Property.COUNTRY_IN_EUROPEAN_UNION,
            Property.COUNTRY_ISO_CODE,
            Property.REGISTERED_COUNTRY_IN_EUROPEAN_UNION,
            Property.REGISTERED_COUNTRY_ISO_CODE,
            Property.REGISTERED_COUNTRY_NAME
        ),
        Set.of(Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE)
    ),
    Asn(
        Set.of(Property.IP, Property.ASN, Property.ORGANIZATION_NAME, Property.NETWORK),
        Set.of(Property.IP, Property.ASN, Property.ORGANIZATION_NAME, Property.NETWORK)
    ),
    AnonymousIp(
        Set.of(
            Property.IP,
            Property.HOSTING_PROVIDER,
            Property.TOR_EXIT_NODE,
            Property.ANONYMOUS_VPN,
            Property.ANONYMOUS,
            Property.PUBLIC_PROXY,
            Property.RESIDENTIAL_PROXY
        ),
        Set.of(
            Property.HOSTING_PROVIDER,
            Property.TOR_EXIT_NODE,
            Property.ANONYMOUS_VPN,
            Property.ANONYMOUS,
            Property.PUBLIC_PROXY,
            Property.RESIDENTIAL_PROXY
        )
    ),
    ConnectionType(Set.of(Property.IP, Property.CONNECTION_TYPE), Set.of(Property.CONNECTION_TYPE)),
    Domain(Set.of(Property.IP, Property.DOMAIN), Set.of(Property.DOMAIN)),
    Enterprise(
        Set.of(
            Property.IP,
            Property.COUNTRY_CONFIDENCE,
            Property.COUNTRY_IN_EUROPEAN_UNION,
            Property.COUNTRY_ISO_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_CODE,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_CONFIDENCE,
            Property.CITY_NAME,
            Property.TIMEZONE,
            Property.LOCATION,
            Property.ASN,
            Property.ORGANIZATION_NAME,
            Property.NETWORK,
            Property.HOSTING_PROVIDER,
            Property.TOR_EXIT_NODE,
            Property.ANONYMOUS_VPN,
            Property.ANONYMOUS,
            Property.PUBLIC_PROXY,
            Property.RESIDENTIAL_PROXY,
            Property.DOMAIN,
            Property.ISP,
            Property.ISP_ORGANIZATION_NAME,
            Property.MOBILE_COUNTRY_CODE,
            Property.MOBILE_NETWORK_CODE,
            Property.USER_TYPE,
            Property.CONNECTION_TYPE,
            Property.POSTAL_CODE,
            Property.POSTAL_CONFIDENCE,
            Property.ACCURACY_RADIUS,
            Property.REGISTERED_COUNTRY_IN_EUROPEAN_UNION,
            Property.REGISTERED_COUNTRY_ISO_CODE,
            Property.REGISTERED_COUNTRY_NAME
        ),
        Set.of(
            Property.COUNTRY_ISO_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.LOCATION
        )
    ),
    Isp(
        Set.of(
            Property.IP,
            Property.ASN,
            Property.ORGANIZATION_NAME,
            Property.NETWORK,
            Property.ISP,
            Property.ISP_ORGANIZATION_NAME,
            Property.MOBILE_COUNTRY_CODE,
            Property.MOBILE_NETWORK_CODE
        ),
        Set.of(
            Property.IP,
            Property.ASN,
            Property.ORGANIZATION_NAME,
            Property.NETWORK,
            Property.ISP,
            Property.ISP_ORGANIZATION_NAME,
            Property.MOBILE_COUNTRY_CODE,
            Property.MOBILE_NETWORK_CODE
        )
    ),
    AsnV2(
        Set.of(
            Property.IP,
            Property.ASN,
            Property.ORGANIZATION_NAME,
            Property.NETWORK,
            Property.DOMAIN,
            Property.COUNTRY_ISO_CODE,
            Property.TYPE
        ),
        Set.of(Property.IP, Property.ASN, Property.ORGANIZATION_NAME, Property.NETWORK)
    ),
    CityV2(
        Set.of(
            Property.IP,
            Property.COUNTRY_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.TIMEZONE,
            Property.LOCATION,
            Property.POSTAL_CODE
        ),
        Set.of(Property.COUNTRY_ISO_CODE, Property.REGION_NAME, Property.CITY_NAME, Property.LOCATION)
    ),
    CountryV2(
        Set.of(Property.IP, Property.CONTINENT_CODE, Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE),
        Set.of(Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE)
    ),
    PrivacyDetection(
        Set.of(Property.IP, Property.HOSTING, Property.PROXY, Property.RELAY, Property.TOR, Property.VPN, Property.SERVICE),
        Set.of(Property.HOSTING, Property.PROXY, Property.RELAY, Property.TOR, Property.VPN, Property.SERVICE)
    );

    private final Set<Property> properties;
    private final Set<Property> defaultProperties;

    Database(Set<Property> properties, Set<Property> defaultProperties) {
        this.properties = properties;
        this.defaultProperties = defaultProperties;
    }

    /**
     * @return a set representing all the valid properties for this database
     */
    public Set<Property> properties() {
        return properties;
    }

    /**
     * @return a set representing the default properties for this database
     */
    public Set<Property> defaultProperties() {
        return defaultProperties;
    }

    /**
     * Parse the given list of property names.
     *
     * @param propertyNames a list of property names to parse, or null to use the default properties for this database
     * @throws IllegalArgumentException if any of the property names are not valid
     * @return a set of parsed and validated properties
     */
    public Set<Property> parseProperties(@Nullable final List<String> propertyNames) {
        if (propertyNames != null) {
            final Set<Property> parsedProperties = new HashSet<>();
            for (String propertyName : propertyNames) {
                parsedProperties.add(Property.parseProperty(this.properties, propertyName)); // n.b. this throws if a property is invalid
            }
            return Set.copyOf(parsedProperties);
        } else {
            // if propertyNames is null, then use the default properties
            return this.defaultProperties;
        }
    }

    /**
     * High-level database 'properties' that represent information that can be extracted from a geoip database.
     */
    enum Property {

        IP("ip", String.class),
        COUNTRY_CONFIDENCE("country_confidence", Integer.class),
        COUNTRY_IN_EUROPEAN_UNION("country_in_european_union", Boolean.class),
        COUNTRY_ISO_CODE("country_iso_code", String.class),
        COUNTRY_NAME("country_name", String.class),
        CONTINENT_CODE("continent_code", String.class),
        CONTINENT_NAME("continent_name", String.class),
        REGION_ISO_CODE("region_iso_code", String.class),
        REGION_NAME("region_name", String.class),
        CITY_CONFIDENCE("city_confidence", Integer.class),
        CITY_NAME("city_name", String.class),
        TIMEZONE("timezone", String.class),
        LOCATION("location", Object.class),
        ASN("asn", Long.class),
        ORGANIZATION_NAME("organization_name", String.class),
        NETWORK("network", String.class),
        HOSTING_PROVIDER("hosting_provider", Boolean.class),
        TOR_EXIT_NODE("tor_exit_node", Boolean.class),
        ANONYMOUS_VPN("anonymous_vpn", Boolean.class),
        ANONYMOUS("anonymous", Boolean.class),
        PUBLIC_PROXY("public_proxy", Boolean.class),
        RESIDENTIAL_PROXY("residential_proxy", Boolean.class),
        DOMAIN("domain", String.class),
        ISP("isp", String.class),
        ISP_ORGANIZATION_NAME("isp_organization_name", String.class),
        MOBILE_COUNTRY_CODE("mobile_country_code", String.class),
        MOBILE_NETWORK_CODE("mobile_network_code", String.class),
        CONNECTION_TYPE("connection_type", String.class),
        USER_TYPE("user_type", String.class),
        TYPE("type", String.class),
        POSTAL_CODE("postal_code", String.class),
        POSTAL_CONFIDENCE("postal_confidence", Integer.class),
        ACCURACY_RADIUS("accuracy_radius", Integer.class),
        HOSTING("hosting", Boolean.class),
        TOR("tor", Boolean.class),
        PROXY("proxy", Boolean.class),
        RELAY("relay", Boolean.class),
        VPN("vpn", Boolean.class),
        SERVICE("service", String.class),
        REGISTERED_COUNTRY_IN_EUROPEAN_UNION("registered_country_in_european_union", Boolean.class),
        REGISTERED_COUNTRY_ISO_CODE("registered_country_iso_code", String.class),
        REGISTERED_COUNTRY_NAME("registered_country_name", String.class);

        private final String fieldName;
        private final Class<?> fieldType;

        Property(String fieldName, Class<?> fieldType) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
        }

        public String fieldName() {
            return fieldName;
        }

        public Class<?> fieldType() {
            return fieldType;
        }

        /**
         * Parses a string representation of a property into an actual Property instance. Not all properties that exist are
         * valid for all kinds of databases, so this method validates the parsed value against the provided set of valid properties.
         * <p>
         * See {@link Database#parseProperties(List)} where this is used.
         *
         * @param validProperties the valid properties against which to validate the parsed property value
         * @param value the string representation to parse
         * @return a parsed, validated Property
         * @throws IllegalArgumentException if the value does not parse as a Property or if the parsed Property is not
         * in the passed-in validProperties set
         */
        private static Property parseProperty(final Set<Property> validProperties, final String value) {
            try {
                Property property = valueOf(value.toUpperCase(Locale.ROOT));
                if (validProperties.contains(property) == false) {
                    throw new IllegalArgumentException("invalid");
                }
                return property;
            } catch (IllegalArgumentException e) {
                // put the properties in natural order before throwing so that we have reliable error messages -- this is a little
                // bit inefficient, but we only do this validation at processor construction time so the cost is practically immaterial
                Property[] properties = validProperties.toArray(new Property[0]);
                Arrays.sort(properties);
                throw new IllegalArgumentException(
                    "illegal property value [" + value + "]. valid values are " + Arrays.toString(properties)
                );
            }
        }
    }
}
