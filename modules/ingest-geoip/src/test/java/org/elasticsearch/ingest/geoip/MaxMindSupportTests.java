/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.Network;
import com.maxmind.geoip2.model.AbstractResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.MaxMind;

import org.elasticsearch.test.ESTestCase;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class MaxMindSupportTests extends ESTestCase {

    public void testMaxMindSupport() {

        Set<String> asnSupportedFields = Set.of("autonomousSystemNumber", "autonomousSystemOrganization", "network");
        Set<String> asnUnsupportedFields = Set.of("ipAddress");

        Set<String> citySupportedFields = Set.of(
            "city.name",
            "continent.name",
            "country.isoCode",
            "country.name",
            "location.latitude",
            "location.longitude",
            "location.timeZone",
            "mostSpecificSubdivision.isoCode",
            "mostSpecificSubdivision.name"
        );
        Set<String> cityUnsupportedFields = Set.of(
            "city.confidence",
            "city.geoNameId",
            "city.names",
            "continent.code",
            "continent.geoNameId",
            "continent.names",
            "country.confidence",
            "country.geoNameId",
            "country.inEuropeanUnion",
            "country.names",
            "leastSpecificSubdivision.confidence",
            "leastSpecificSubdivision.geoNameId",
            "leastSpecificSubdivision.isoCode",
            "leastSpecificSubdivision.name",
            "leastSpecificSubdivision.names",
            "location.accuracyRadius",
            "location.averageIncome",
            "location.metroCode",
            "location.populationDensity",
            "maxMind",
            "mostSpecificSubdivision.confidence",
            "mostSpecificSubdivision.geoNameId",
            "mostSpecificSubdivision.names",
            "postal.code",
            "postal.confidence",
            "registeredCountry.confidence",
            "registeredCountry.geoNameId",
            "registeredCountry.inEuropeanUnion",
            "registeredCountry.isoCode",
            "registeredCountry.name",
            "registeredCountry.names",
            "representedCountry.confidence",
            "representedCountry.geoNameId",
            "representedCountry.inEuropeanUnion",
            "representedCountry.isoCode",
            "representedCountry.name",
            "representedCountry.names",
            "representedCountry.type",
            "subdivisions.confidence",
            "subdivisions.geoNameId",
            "subdivisions.isoCode",
            "subdivisions.name",
            "subdivisions.names",
            "traits.anonymous",
            "traits.anonymousProxy",
            "traits.anonymousVpn",
            "traits.anycast",
            "traits.autonomousSystemNumber",
            "traits.autonomousSystemOrganization",
            "traits.connectionType",
            "traits.domain",
            "traits.hostingProvider",
            "traits.ipAddress",
            "traits.isp",
            "traits.legitimateProxy",
            "traits.mobileCountryCode",
            "traits.mobileNetworkCode",
            "traits.network",
            "traits.organization",
            "traits.publicProxy",
            "traits.residentialProxy",
            "traits.satelliteProvider",
            "traits.staticIpScore",
            "traits.torExitNode",
            "traits.userCount",
            "traits.userType"
        );

        Set<String> countrySupportedFields = Set.of("continent.name", "country.isoCode", "country.name");
        Set<String> countryUnsupportedFields = Set.of(
            "continent.code",
            "continent.geoNameId",
            "continent.names",
            "country.confidence",
            "country.geoNameId",
            "country.inEuropeanUnion",
            "country.names",
            "maxMind",
            "registeredCountry.confidence",
            "registeredCountry.geoNameId",
            "registeredCountry.inEuropeanUnion",
            "registeredCountry.isoCode",
            "registeredCountry.name",
            "registeredCountry.names",
            "representedCountry.confidence",
            "representedCountry.geoNameId",
            "representedCountry.inEuropeanUnion",
            "representedCountry.isoCode",
            "representedCountry.name",
            "representedCountry.names",
            "representedCountry.type",
            "traits.anonymous",
            "traits.anonymousProxy",
            "traits.anonymousVpn",
            "traits.anycast",
            "traits.autonomousSystemNumber",
            "traits.autonomousSystemOrganization",
            "traits.connectionType",
            "traits.domain",
            "traits.hostingProvider",
            "traits.ipAddress",
            "traits.isp",
            "traits.legitimateProxy",
            "traits.mobileCountryCode",
            "traits.mobileNetworkCode",
            "traits.network",
            "traits.organization",
            "traits.publicProxy",
            "traits.residentialProxy",
            "traits.satelliteProvider",
            "traits.staticIpScore",
            "traits.torExitNode",
            "traits.userCount",
            "traits.userType"
        );

        Map<Database, Set<String>> typeToSupportedFieldsMap = Map.of(
            Database.Asn,
            asnSupportedFields,
            Database.City,
            citySupportedFields,
            Database.Country,
            countrySupportedFields
        );
        Map<Database, Set<String>> typeToUnsupportedFieldsMap = Map.of(
            Database.Asn,
            asnUnsupportedFields,
            Database.City,
            cityUnsupportedFields,
            Database.Country,
            countryUnsupportedFields
        );
        Map<Database, Class<? extends AbstractResponse>> typeToMaxMindClass = Map.of(
            Database.Asn,
            AsnResponse.class,
            Database.City,
            CityResponse.class,
            Database.Country,
            CountryResponse.class
        );

        for (Database databaseType : Database.values()) {
            Class<? extends AbstractResponse> maxMindClass = typeToMaxMindClass.get(databaseType);
            Set<String> supportedFields = typeToSupportedFieldsMap.get(databaseType);
            Set<String> unsupportedFields = typeToUnsupportedFieldsMap.get(databaseType);
            assertNotNull(
                "A new Database type, "
                    + databaseType
                    + ", has been added, but this test has not been updated to know which MaxMind "
                    + "class to use to load it. Update typeToMaxMindClass",
                maxMindClass
            );
            assertNotNull(
                "A new Database type, "
                    + databaseType
                    + ", has been added, but this test has not been updated to know which fields we "
                    + "support for it. Update typeToSupportedFieldsMap",
                supportedFields
            );
            assertNotNull(
                "A new Database type, "
                    + databaseType
                    + ", has been added, but this test has not been updated to know which fields we "
                    + "do not support for it. Update typeToUnsupportedFieldsMap",
                unsupportedFields
            );
            final SortedSet<String> fieldNames = getFieldNamesUsedFromClass(maxMindClass);
            SortedSet<String> unusedFields = new TreeSet<>(fieldNames);
            unusedFields.removeAll(supportedFields);
            assertThat(
                "New MaxMind fields have been added for "
                    + databaseType
                    + " that we do not use or have documented in typeToUnsupportedFieldsMap. The actual list of fields is:\n"
                    + getFormattedList(unusedFields),
                unusedFields,
                equalTo(new TreeSet<>(unsupportedFields))
            );
            SortedSet<String> nonexistentFields = new TreeSet<>(supportedFields);
            nonexistentFields.removeAll(fieldNames);
            assertThat(
                "We are attempting to use fields that MaxMind does not support for " + databaseType,
                nonexistentFields.size(),
                equalTo(0)
            );
        }
    }

    /*
     * This is the list of field types that causes us to stop recursing. That is, fields of these types are the lowest-level fields that
     * we care about.
     */
    private static final Set<Class<?>> TERMINAL_TYPES = Set.of(
        boolean.class,
        Boolean.class,
        char.class,
        Character.class,
        Class.class,
        ConnectionTypeResponse.ConnectionType.class,
        double.class,
        Double.class,
        InetAddress.class,
        int.class,
        Integer.class,
        long.class,
        Long.class,
        MaxMind.class,
        Network.class,
        Object.class,
        String.class,
        void.class,
        Void.class
    );
    /*
     * These are types that are containers for other types. We don't need to recurse into each method on these types. Instead, we need to
     *  look at their generic types.
     */
    private static final Set<Class<?>> CONTAINER_TYPES = Set.of(Collection.class, List.class, Map.class, Optional.class);
    /*
     * These are methods we don't want to traverse into.
     */
    private static final Set<Method> IGNORED_METHODS = Arrays.stream(Object.class.getMethods()).collect(Collectors.toUnmodifiableSet());

    /*
     * Returns the set of bean-property-like field names referenced from aClass, sorted alphabetically.
     */
    private static SortedSet<String> getFieldNamesUsedFromClass(Class<?> aClass) {
        SortedSet<String> fieldNames = new TreeSet<>();
        getFieldNamesUsedFromClass(fieldNames, "", aClass);
        return fieldNames;
    }

    private static void getFieldNamesUsedFromClass(Set<String> fieldsSet, String context, Class<?> aClass) {
        Method[] methods = aClass.getMethods();
        if (TERMINAL_TYPES.contains(aClass)) {
            // We got here becaus a collection type had a terminal type
            fieldsSet.add(context);
            return;
        }
        for (Method method : methods) {
            if (IGNORED_METHODS.contains(method)) {
                continue;
            }
            if (method.getName().startsWith("to")) {
                // ignoring methods like toJson or toString
                continue;
            }
            String currentContext = context + (context.isEmpty() ? "" : ".") + beanify(method.getName());
            if (TERMINAL_TYPES.contains(method.getReturnType())) {
                fieldsSet.add(currentContext);
            } else {
                Class<?> returnType = method.getReturnType();
                if (CONTAINER_TYPES.contains(returnType)) {
                    ParameterizedType genericReturnType = (ParameterizedType) method.getGenericReturnType();
                    for (Type actualType : genericReturnType.getActualTypeArguments()) {
                        if (actualType instanceof Class<?> actualTypeClass) {
                            getFieldNamesUsedFromClass(fieldsSet, currentContext, actualTypeClass);
                        } else {
                            assert false : "This test needs to be updated to deal with this situation";
                        }
                    }
                } else {
                    getFieldNamesUsedFromClass(fieldsSet, currentContext, method.getReturnType());
                }
            }
        }
    }

    /*
     * This method converts a method name into what would be its equivalent bean property. For example "getName" returns "name".
     */
    private static String beanify(String methodName) {
        if (methodName.startsWith("get")) {
            return beanify("get", methodName);
        } else if (methodName.startsWith("is")) {
            return beanify("is", methodName);
        } else {
            return methodName;
        }
    }

    private static String beanify(String prefix, String methodName) {
        return methodName.substring(prefix.length(), prefix.length() + 1).toLowerCase() + methodName.substring(prefix.length() + 1);
    }

    /*
     * This is a convenience to format the list of field names in fields into a String that can be copied into a Set initializer above,
     * like countryUnsupportedFields.
     */
    private static String getFormattedList(Set<String> fields) {
        StringBuilder result = new StringBuilder();
        for (Iterator<String> it = fields.iterator(); it.hasNext();) {
            result.append("\"");
            result.append(it.next());
            result.append("\"");
            if (it.hasNext()) {
                result.append(",\n");
            }
        }
        return result.toString();
    }
}
