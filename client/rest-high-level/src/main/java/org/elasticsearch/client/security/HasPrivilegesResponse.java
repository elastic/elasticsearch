/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Response when checking whether the current user has a defined set of privileges.
 */
public final class HasPrivilegesResponse {

    private static final ConstructingObjectParser<HasPrivilegesResponse, Void> PARSER = new ConstructingObjectParser<>(
        "has_privileges_response", true, args -> new HasPrivilegesResponse(
        (String) args[0], (Boolean) args[1], checkMap(args[2], 0), checkMap(args[3], 1), checkMap(args[4], 2)));

    static {
        PARSER.declareString(constructorArg(), new ParseField("username"));
        PARSER.declareBoolean(constructorArg(), new ParseField("has_all_requested"));
        declareMap(constructorArg(), "cluster");
        declareMap(constructorArg(), "index");
        declareMap(constructorArg(), "application");
    }

    @SuppressWarnings("unchecked")
    private static <T> Map<String, T> checkMap(Object argument, int depth) {
        if (argument instanceof Map) {
            Map<String, T> map = (Map<String, T>) argument;
            if (depth == 0) {
                map.values().stream()
                    .filter(val -> (val instanceof Boolean) == false)
                    .forEach(val -> {
                        throw new IllegalArgumentException("Map value [" + val + "] in [" + map + "] is not a Boolean");
                    });
            } else {
                map.values().stream().forEach(val -> checkMap(val, depth - 1));
            }
            return map;
        }
        throw new IllegalArgumentException("Value [" + argument + "] is not an Object");
    }

    private static void declareMap(BiConsumer<HasPrivilegesResponse, Map<String, Object>> arg, String name) {
        PARSER.declareField(arg, XContentParser::map, new ParseField(name), ObjectParser.ValueType.OBJECT);
    }

    private final String username;
    private final boolean hasAllRequested;
    private final Map<String, Boolean> clusterPrivileges;
    private final Map<String, Map<String, Boolean>> indexPrivileges;
    private final Map<String, Map<String, Map<String, Boolean>>> applicationPrivileges;

    public HasPrivilegesResponse(String username, boolean hasAllRequested,
                                 Map<String, Boolean> clusterPrivileges,
                                 Map<String, Map<String, Boolean>> indexPrivileges,
                                 Map<String, Map<String, Map<String, Boolean>>> applicationPrivileges) {
        this.username = username;
        this.hasAllRequested = hasAllRequested;
        this.clusterPrivileges = Collections.unmodifiableMap(clusterPrivileges);
        this.indexPrivileges = unmodifiableMap2(indexPrivileges);
        this.applicationPrivileges = unmodifiableMap3(applicationPrivileges);
    }

    private static Map<String, Map<String, Boolean>> unmodifiableMap2(final Map<String, Map<String, Boolean>> map) {
        final Map<String, Map<String, Boolean>> copy = new HashMap<>(map);
        copy.replaceAll((k, v) -> Collections.unmodifiableMap(v));
        return Collections.unmodifiableMap(copy);
    }

    private static Map<String, Map<String, Map<String, Boolean>>> unmodifiableMap3(
        final Map<String, Map<String, Map<String, Boolean>>> map) {
        final Map<String, Map<String, Map<String, Boolean>>> copy = new HashMap<>(map);
        copy.replaceAll((k, v) -> unmodifiableMap2(v));
        return Collections.unmodifiableMap(copy);
    }

    public static HasPrivilegesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * The username (principal) of the user for which the privileges check was executed.
     */
    public String getUsername() {
        return username;
    }

    /**
     * {@code true} if the user has every privilege that was checked. Otherwise {@code false}.
     */
    public boolean hasAllRequested() {
        return hasAllRequested;
    }

    /**
     * @param clusterPrivilegeName The name of a cluster privilege. This privilege must have been specified (verbatim) in the
     *                             {@link HasPrivilegesRequest#getClusterPrivileges() cluster privileges of the request}.
     * @return {@code true} if the user has the specified cluster privilege. {@code false} if the privilege was checked
     * but it has not been granted to the user.
     * @throws IllegalArgumentException if the response did not include a value for the specified privilege name.
     *                                  The response only includes values for privileges that were
     *                                  {@link HasPrivilegesRequest#getClusterPrivileges() included in the request}.
     */
    public boolean hasClusterPrivilege(String clusterPrivilegeName) throws IllegalArgumentException {
        Boolean has = clusterPrivileges.get(clusterPrivilegeName);
        if (has == null) {
            throw new IllegalArgumentException("Cluster privilege [" + clusterPrivilegeName + "] was not included in this response");
        }
        return has;
    }

    /**
     * @param indexName     The name of the index to check. This index must have been specified (verbatim) in the
     *                      {@link HasPrivilegesRequest#getIndexPrivileges() requested index privileges}.
     * @param privilegeName The name of the index privilege to check. This privilege must have been specified (verbatim), for the
     *                      given index, in the {@link HasPrivilegesRequest#getIndexPrivileges() requested index privileges}.
     * @return {@code true} if the user has the specified privilege on the specified index. {@code false} if the privilege was checked
     * for that index and was not granted to the user.
     * @throws IllegalArgumentException if the response did not include a value for the specified index and privilege name pair.
     *                                  The response only includes values for indices and privileges that were
     *                                  {@link HasPrivilegesRequest#getIndexPrivileges() included in the request}.
     */
    public boolean hasIndexPrivilege(String indexName, String privilegeName) {
        Map<String, Boolean> indexPrivileges = this.indexPrivileges.get(indexName);
        if (indexPrivileges == null) {
            throw new IllegalArgumentException("No privileges for index [" + indexName + "] were included in this response");
        }
        Boolean has = indexPrivileges.get(privilegeName);
        if (has == null) {
            throw new IllegalArgumentException("Privilege [" + privilegeName + "] was not included in the response for index ["
                + indexName + "]");
        }
        return has;
    }

    /**
     * @param applicationName The name of the application to check. This application must have been specified (verbatim) in the
     *                        {@link HasPrivilegesRequest#getApplicationPrivileges() requested application privileges}.
     * @param resourceName    The name of the resource to check. This resource must have been specified (verbatim), for the given
     *                        application in the {@link HasPrivilegesRequest#getApplicationPrivileges() requested application privileges}.
     * @param privilegeName   The name of the privilege to check. This privilege must have been specified (verbatim), for the given
     *                        application and resource, in the
     *                        {@link HasPrivilegesRequest#getApplicationPrivileges() requested application privileges}.
     * @return {@code true} if the user has the specified privilege on the specified resource in the specified application.
     * {@code false} if the privilege was checked for that application and resource, but was not granted to the user.
     * @throws IllegalArgumentException if the response did not include a value for the specified application, resource and privilege
     *                                  triplet. The response only includes values for applications, resources and privileges that were
     *                                  {@link HasPrivilegesRequest#getApplicationPrivileges() included in the request}.
     */
    public boolean hasApplicationPrivilege(String applicationName, String resourceName, String privilegeName) {
        final Map<String, Map<String, Boolean>> appPrivileges = this.applicationPrivileges.get(applicationName);
        if (appPrivileges == null) {
            throw new IllegalArgumentException("No privileges for application [" + applicationName + "] were included in this response");
        }
        final Map<String, Boolean> resourcePrivileges = appPrivileges.get(resourceName);
        if (resourcePrivileges == null) {
            throw new IllegalArgumentException("No privileges for resource [" + resourceName +
                "] were included in the response for application [" + applicationName + "]");
        }
        Boolean has = resourcePrivileges.get(privilegeName);
        if (has == null) {
            throw new IllegalArgumentException("Privilege [" + privilegeName + "] was not included in the response for application [" +
                applicationName + "] and resource [" + resourceName + "]");
        }
        return has;
    }

    /**
     * A {@code Map} from cluster-privilege-name to access. Each requested privilege is included as a key in the map, and the
     * associated value indicates whether the user was granted that privilege.
     * <p>
     * The {@link #hasClusterPrivilege} method should be used in preference to directly accessing this map.
     * </p>
     */
    public Map<String, Boolean> getClusterPrivileges() {
        return clusterPrivileges;
    }

    /**
     * A {@code Map} from index-name + privilege-name to access. Each requested index is a key in the outer map.
     * Each requested privilege is a key in the inner map. The inner most {@code Boolean} value indicates whether
     * the user was granted that privilege on that index.
     * <p>
     * The {@link #hasIndexPrivilege} method should be used in preference to directly accessing this map.
     * </p>
     */
    public Map<String, Map<String, Boolean>> getIndexPrivileges() {
        return indexPrivileges;
    }

    /**
     * A {@code Map} from application-name + resource-name + privilege-name to access. Each requested application is a key in the
     * outer-most map. Each requested resource is a key in the next-level map. The requested privileges form the keys in the inner-most map.
     * The {@code Boolean} value indicates whether the user was granted that privilege on that resource within that application.
     * <p>
     * The {@link #hasApplicationPrivilege} method should be used in preference to directly accessing this map.
     * </p>
     */
    public Map<String, Map<String, Map<String, Boolean>>> getApplicationPrivileges() {
        return applicationPrivileges;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || this.getClass() != o.getClass()) {
            return false;
        }
        final HasPrivilegesResponse that = (HasPrivilegesResponse) o;
        return this.hasAllRequested == that.hasAllRequested &&
            Objects.equals(this.username, that.username) &&
            Objects.equals(this.clusterPrivileges, that.clusterPrivileges) &&
            Objects.equals(this.indexPrivileges, that.indexPrivileges) &&
            Objects.equals(this.applicationPrivileges, that.applicationPrivileges);
    }

    @Override
    public int hashCode() {
        return Objects.hash(username, hasAllRequested, clusterPrivileges, indexPrivileges, applicationPrivileges);
    }
}

