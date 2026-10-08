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
 * A collector interface for all IP location properties. Each method corresponds 1:1
 * to a {@link DatabaseProperty} enum value. The core lookup logic pushes results to
 * the collector, and each consumer provides its own implementation.
 * <p>
 * Methods are intentionally abstract (no defaults) so that adding a new
 * {@link DatabaseProperty} and its corresponding method here forces a compile error
 * in all implementors, ensuring they handle the new property.
 * <p>
 * Ingest uses {@link IpLocationInfoMapCollector} (puts field values into a map).
 * ES|QL uses a block-writing collector that writes directly to output blocks,
 * avoiding per-row Map allocation.
 */
public interface IpLocationInfoCollector {

    void ip(String ip);

    void countryInEuropeanUnion(boolean val);

    void countryIsoCode(String code);

    void countryName(String name);

    void continentCode(String code);

    void continentName(String name);

    void regionIsoCode(String code);

    void regionName(String name);

    void cityName(String name);

    void timezone(String tz);

    void location(double lat, double lon);

    void postalCode(String code);

    void accuracyRadius(int radius);

    void asn(long asn);

    void organizationName(String name);

    void network(String network);

    void hostingProvider(boolean val);

    void torExitNode(boolean val);

    void anonymousVpn(boolean val);

    void anonymous(boolean val);

    void publicProxy(boolean val);

    void residentialProxy(boolean val);

    void domain(String domain);

    void connectionType(String type);

    void isp(String isp);

    void ispOrganizationName(String name);

    void mobileCountryCode(String code);

    void mobileNetworkCode(String code);

    void userType(String type);

    void type(String type);

    void countryConfidence(int confidence);

    void cityConfidence(int confidence);

    void postalConfidence(int confidence);

    void registeredCountryInEuropeanUnion(boolean val);

    void registeredCountryIsoCode(String code);

    void registeredCountryName(String name);

    void hosting(boolean val);

    void proxy(boolean val);

    void relay(boolean val);

    void tor(boolean val);

    void vpn(boolean val);

    void service(String service);

    void anycast(boolean val);

    void mobile(boolean val);

    void satellite(boolean val);

    void dmaCode(String code);

    void geonameId(String id);

    void asnChangedDate(String date);

    void geoChangedDate(String date);
}
