/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

/**
 * A flat collector interface with default no-op methods for all IP location
 * properties. The core lookup logic pushes results to the collector, and each
 * consumer provides its own implementation.
 * <p>
 * Ingest uses {@link IpLocationInfoMapCollector} (puts field values into a map).
 * ES|QL can use a block-writing collector that writes directly to output blocks,
 * avoiding per-row Map allocation.
 */
public interface IpLocationInfoCollector {

    default void ip(String ip) {}

    default void countryInEuropeanUnion(boolean val) {}

    default void countryIsoCode(String code) {}

    default void countryName(String name) {}

    default void continentCode(String code) {}

    default void continentName(String name) {}

    default void regionIsoCode(String code) {}

    default void regionName(String name) {}

    default void cityName(String name) {}

    default void timezone(String tz) {}

    default void location(double lat, double lon) {}

    default void postalCode(String code) {}

    default void accuracyRadius(int radius) {}

    default void asn(long asn) {}

    default void organizationName(String name) {}

    default void network(String network) {}

    default void hostingProvider(boolean val) {}

    default void torExitNode(boolean val) {}

    default void anonymousVpn(boolean val) {}

    default void anonymous(boolean val) {}

    default void publicProxy(boolean val) {}

    default void residentialProxy(boolean val) {}

    default void domain(String domain) {}

    default void connectionType(String type) {}

    default void isp(String isp) {}

    default void ispOrganizationName(String name) {}

    default void mobileCountryCode(String code) {}

    default void mobileNetworkCode(String code) {}

    default void userType(String type) {}

    default void type(String type) {}

    default void countryConfidence(int confidence) {}

    default void cityConfidence(int confidence) {}

    default void postalConfidence(int confidence) {}

    default void registeredCountryInEuropeanUnion(boolean val) {}

    default void registeredCountryIsoCode(String code) {}

    default void registeredCountryName(String name) {}

    default void hosting(boolean val) {}

    default void proxy(boolean val) {}

    default void relay(boolean val) {}

    default void tor(boolean val) {}

    default void vpn(boolean val) {}

    default void service(String service) {}
}
