/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * A high-level representation of a kind of geoip database that is supported by the {@link GeoIpProcessor}.
 * <p>
 * A database has a set of properties that are valid to use with it (see {@link Database#properties()}),
 * as well as a list of default properties to use if no properties are specified (see {@link Database#defaultProperties()}).
 * <p>
 * See especially {@link Database#getDatabase(String, String)} which is used to obtain instances of this class.
 */
enum Database {

    City(
        Set.of(
            Property.IP,
            Property.COUNTRY_ISO_CODE,
            Property.CONTINENT_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.TIMEZONE,
            Property.LOCATION
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
        Set.of(Property.IP, Property.CONTINENT_CODE, Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE),
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
            Property.COUNTRY_ISO_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_CODE,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
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
            Property.CONNECTION_TYPE
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
    );

    private static final String CITY_DB_SUFFIX = "-City";
    private static final String COUNTRY_DB_SUFFIX = "-Country";
    private static final String ASN_DB_SUFFIX = "-ASN";
    private static final String ANONYMOUS_IP_DB_SUFFIX = "-Anonymous-IP";
    private static final String CONNECTION_TYPE_DB_SUFFIX = "-Connection-Type";
    private static final String DOMAIN_DB_SUFFIX = "-Domain";
    private static final String ENTERPRISE_DB_SUFFIX = "-Enterprise";
    private static final String ISP_DB_SUFFIX = "-ISP";

    @Nullable
    private static Database getMaxmindDatabase(final String databaseType) {
        if (databaseType.endsWith(Database.CITY_DB_SUFFIX)) {
            return Database.City;
        } else if (databaseType.endsWith(Database.COUNTRY_DB_SUFFIX)) {
            return Database.Country;
        } else if (databaseType.endsWith(Database.ASN_DB_SUFFIX)) {
            return Database.Asn;
        } else if (databaseType.endsWith(Database.ANONYMOUS_IP_DB_SUFFIX)) {
            return Database.AnonymousIp;
        } else if (databaseType.endsWith(Database.CONNECTION_TYPE_DB_SUFFIX)) {
            return Database.ConnectionType;
        } else if (databaseType.endsWith(Database.DOMAIN_DB_SUFFIX)) {
            return Database.Domain;
        } else if (databaseType.endsWith(Database.ENTERPRISE_DB_SUFFIX)) {
            return Database.Enterprise;
        } else if (databaseType.endsWith(Database.ISP_DB_SUFFIX)) {
            return Database.Isp;
        } else {
            return null; // no match was found
        }
    }

    /**
     * Parses the passed-in databaseType (presumably from the passed-in databaseFile) and return the Database instance that is
     * associated with that databaseType.
     *
     * @param databaseType the database type String from the metadata of the database file
     * @param databaseFile the database file from which the database type was obtained
     * @throws IllegalArgumentException if the databaseType is not associated with a Database instance
     * @return the Database instance that is associated with the databaseType
     */
    public static Database getDatabase(final String databaseType, final String databaseFile) {
        Database database = null;

        if (Strings.hasText(databaseType)) {
            database = getMaxmindDatabase(databaseType);
        }

        if (database == null) {
            throw new IllegalArgumentException("Unsupported database type [" + databaseType + "] for file [" + databaseFile + "]");
        }

        return database;
    }

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

        IP,
        COUNTRY_ISO_CODE,
        COUNTRY_NAME,
        CONTINENT_CODE,
        CONTINENT_NAME,
        REGION_ISO_CODE,
        REGION_NAME,
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
        DOMAIN,
        ISP,
        ISP_ORGANIZATION_NAME,
        MOBILE_COUNTRY_CODE,
        MOBILE_NETWORK_CODE,
        CONNECTION_TYPE,
        USER_TYPE;

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
