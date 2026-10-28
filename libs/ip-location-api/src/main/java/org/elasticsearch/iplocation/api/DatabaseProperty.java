/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;

/**
 * High-level database properties that represent information that can be extracted from an IP location database.
 * These correspond 1:1 with the methods on {@link IpLocationInfoCollector}.
 */
public enum DatabaseProperty {

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
    REGISTERED_COUNTRY_NAME("registered_country_name", String.class),
    ANYCAST("anycast", Boolean.class),
    MOBILE("mobile", Boolean.class),
    SATELLITE("satellite", Boolean.class),
    DMA_CODE("dma_code", String.class),
    GEONAME_ID("geoname_id", String.class),
    ASN_CHANGED_DATE("asn_changed_date", String.class),
    GEO_CHANGED_DATE("geo_changed_date", String.class);

    private final String fieldName;
    private final Class<?> fieldType;

    DatabaseProperty(String fieldName, Class<?> fieldType) {
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
     * Builds the set of {@link DatabaseProperty} values whose {@link #fieldName()} is present in the given set of field names.
     * Useful for bridging between the string-keyed field map from {@link IpDataLookupInfo#getFields()} and the enum-keyed
     * validation required by {@link #parseProperty(Set, String)}.
     *
     * @param fieldNames the set of valid field name strings (e.g. from {@code info.getFields().keySet()})
     * @return the subset of {@link DatabaseProperty} values matching the given field names
     */
    public static Set<DatabaseProperty> buildValidSet(Set<String> fieldNames) {
        Set<DatabaseProperty> result = EnumSet.noneOf(DatabaseProperty.class);
        for (DatabaseProperty dp : values()) {
            if (fieldNames.contains(dp.fieldName())) {
                result.add(dp);
            }
        }
        return result;
    }

    /**
     * Parses a string representation of a property into an actual {@link DatabaseProperty} instance. Not all properties
     * that exist are valid for all kinds of databases, so this method validates the parsed value against the provided
     * set of valid properties.
     *
     * @param validProperties the valid properties against which to validate the parsed property value
     * @param value the string representation to parse
     * @return a parsed, validated DatabaseProperty
     * @throws IllegalArgumentException if the value does not parse as a DatabaseProperty or if the parsed value is not
     * in the passed-in validProperties set
     */
    public static DatabaseProperty parseProperty(final Set<DatabaseProperty> validProperties, final String value) {
        try {
            DatabaseProperty property = valueOf(value.toUpperCase(Locale.ROOT));
            if (validProperties.contains(property) == false) {
                throw new IllegalArgumentException("invalid");
            }
            return property;
        } catch (IllegalArgumentException e) {
            // put the properties in natural order before throwing so that we have reliable error messages -- this is a little
            // bit inefficient, but we only do this validation at processor construction time so the cost is practically immaterial
            DatabaseProperty[] properties = validProperties.toArray(new DatabaseProperty[0]);
            Arrays.sort(properties);
            throw new IllegalArgumentException("illegal property value [" + value + "]. valid values are " + Arrays.toString(properties));
        }
    }
}
