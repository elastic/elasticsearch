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

public final class Database {

    public static final String CITY_DB_SUFFIX = "-City";
    public static final String COUNTRY_DB_SUFFIX = "-Country";
    public static final String ASN_DB_SUFFIX = "-ASN";

    private Database() {
        // utility class
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

        static final Set<Property> ALL_CITY_PROPERTIES = Set.of(
            Property.IP,
            Property.COUNTRY_ISO_CODE,
            Property.COUNTRY_NAME,
            Property.CONTINENT_NAME,
            Property.REGION_ISO_CODE,
            Property.REGION_NAME,
            Property.CITY_NAME,
            Property.TIMEZONE,
            Property.LOCATION
        );
        static final Set<Property> ALL_COUNTRY_PROPERTIES = Set.of(
            Property.IP,
            Property.CONTINENT_NAME,
            Property.COUNTRY_NAME,
            Property.COUNTRY_ISO_CODE
        );
        static final Set<Property> ALL_ASN_PROPERTIES = Set.of(Property.IP, Property.ASN, Property.ORGANIZATION_NAME, Property.NETWORK);

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

        /**
         * Parse the given list of property names and validate them against the supplied databaseType.
         *
         * @param databaseType the type of database to use to validate property names
         * @param propertyNames a list of property names to parse, or null to use the default properties for the associated databaseType
         * @throws IllegalArgumentException if any of the property names are not valid, or if the databaseType is not valid
         * @return a set of parsed and validated properties
         */
        public static Set<Property> parseProperties(final String databaseType, @Nullable final List<String> propertyNames) {
            final Set<Property> validProperties;
            final Set<Property> defaultProperties;

            if (databaseType.endsWith(CITY_DB_SUFFIX)) {
                validProperties = ALL_CITY_PROPERTIES;
                defaultProperties = GeoIpProcessor.Factory.DEFAULT_CITY_PROPERTIES;
            } else if (databaseType.endsWith(COUNTRY_DB_SUFFIX)) {
                validProperties = ALL_COUNTRY_PROPERTIES;
                defaultProperties = GeoIpProcessor.Factory.DEFAULT_COUNTRY_PROPERTIES;
            } else if (databaseType.endsWith(ASN_DB_SUFFIX)) {
                validProperties = ALL_ASN_PROPERTIES;
                defaultProperties = GeoIpProcessor.Factory.DEFAULT_ASN_PROPERTIES;
            } else {
                assert false : "Unsupported database type [" + databaseType + "]";
                throw new IllegalArgumentException("Unsupported database type [" + databaseType + "]");
            }

            final Set<Property> properties;
            if (propertyNames != null) {
                Set<Property> modifiableProperties = new HashSet<>();
                for (String propertyName : propertyNames) {
                    modifiableProperties.add(parseProperty(validProperties, propertyName)); // n.b. this throws if a property is invalid
                }
                properties = Set.copyOf(modifiableProperties);
            } else {
                // if propertyNames is null, then use the default properties for the databaseType
                properties = defaultProperties;
            }
            return properties;
        }
    }
}
