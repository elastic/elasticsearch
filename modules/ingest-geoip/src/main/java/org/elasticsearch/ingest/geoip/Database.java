/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.core.Nullable;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public enum Database {

    City(
        Set.of(
            Property.IP,
            Property.COUNTRY_ISO_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.TIMEZONE,
            Property.LOCATION
        ),
        Set.of(
            Property.CONTINENT_NAME,
            Property.COUNTRY_NAME,
            Property.COUNTRY_ISO_CODE,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.LOCATION
        )
    ),
    Country(
        Set.of(Property.IP, Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE),
        Set.of(Property.CONTINENT_NAME, Property.COUNTRY_NAME, Property.COUNTRY_ISO_CODE)
    ),
    Asn(
        Set.of(Property.IP, Property.ASN, Property.ORGANIZATION_NAME, Property.NETWORK),
        Set.of(Property.IP, Property.ASN, Property.ORGANIZATION_NAME, Property.NETWORK)
    );

    private static final String CITY_DB_SUFFIX = "-City";
    private static final String COUNTRY_DB_SUFFIX = "-Country";
    private static final String ASN_DB_SUFFIX = "-ASN";

    public static Database getDatabase(final String databaseType, final String databaseFile) {
        Database database = null;
        if (databaseType != null) {
            if (databaseType.endsWith(Database.CITY_DB_SUFFIX)) {
                database = Database.City;
            } else if (databaseType.endsWith(Database.COUNTRY_DB_SUFFIX)) {
                database = Database.Country;
            } else if (databaseType.endsWith(Database.ASN_DB_SUFFIX)) {
                database = Database.Asn;
            }
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

    public Set<Property> properties() {
        return properties;
    }

    public Set<Property> defaultProperties() {
        return defaultProperties;
    }

    /**
     * Parse the given list of property names and validate them against the supplied databaseType.
     *
     * @param propertyNames a list of property names to parse, or null to use the default properties for the associated databaseType
     * @throws IllegalArgumentException if any of the property names are not valid, or if the databaseType is not valid
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

    public enum Property {

        IP,
        COUNTRY_ISO_CODE,
        COUNTRY_NAME,
        CONTINENT_NAME,
        REGION_ISO_CODE,
        REGION_NAME,
        CITY_NAME,
        TIMEZONE,
        LOCATION,
        ASN,
        ORGANIZATION_NAME,
        NETWORK;

        private static Property parseProperty(Set<Property> validProperties, String value) {
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
