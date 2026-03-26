/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.iplocation.api;

import java.util.HashMap;
import java.util.Map;

/**
 * Default map-based collector for IP location data. Each method puts the value
 * into the map keyed by the field name string. Used by the ingest processor for
 * simple Map-based document enrichment.
 */
public class IpLocationInfoMapCollector extends HashMap<String, Object> implements IpLocationInfoCollector {

    @Override
    public void ip(String ip) {
        put("ip", ip);
    }

    @Override
    public void countryInEuropeanUnion(boolean val) {
        put("country_in_european_union", val);
    }

    @Override
    public void countryIsoCode(String code) {
        put("country_iso_code", code);
    }

    @Override
    public void countryName(String name) {
        put("country_name", name);
    }

    @Override
    public void continentCode(String code) {
        put("continent_code", code);
    }

    @Override
    public void continentName(String name) {
        put("continent_name", name);
    }

    @Override
    public void regionIsoCode(String code) {
        put("region_iso_code", code);
    }

    @Override
    public void regionName(String name) {
        put("region_name", name);
    }

    @Override
    public void cityName(String name) {
        put("city_name", name);
    }

    @Override
    public void timezone(String tz) {
        put("timezone", tz);
    }

    @Override
    public void location(double lat, double lon) {
        put("location", Map.of("lat", lat, "lon", lon));
    }

    @Override
    public void postalCode(String code) {
        put("postal_code", code);
    }

    @Override
    public void accuracyRadius(int radius) {
        put("accuracy_radius", radius);
    }

    @Override
    public void asn(long asn) {
        put("asn", asn);
    }

    @Override
    public void organizationName(String name) {
        put("organization_name", name);
    }

    @Override
    public void network(String network) {
        put("network", network);
    }

    @Override
    public void hostingProvider(boolean val) {
        put("hosting_provider", val);
    }

    @Override
    public void torExitNode(boolean val) {
        put("tor_exit_node", val);
    }

    @Override
    public void anonymousVpn(boolean val) {
        put("anonymous_vpn", val);
    }

    @Override
    public void anonymous(boolean val) {
        put("anonymous", val);
    }

    @Override
    public void publicProxy(boolean val) {
        put("public_proxy", val);
    }

    @Override
    public void residentialProxy(boolean val) {
        put("residential_proxy", val);
    }

    @Override
    public void domain(String domain) {
        put("domain", domain);
    }

    @Override
    public void connectionType(String type) {
        put("connection_type", type);
    }

    @Override
    public void isp(String isp) {
        put("isp", isp);
    }

    @Override
    public void ispOrganizationName(String name) {
        put("isp_organization_name", name);
    }

    @Override
    public void mobileCountryCode(String code) {
        put("mobile_country_code", code);
    }

    @Override
    public void mobileNetworkCode(String code) {
        put("mobile_network_code", code);
    }

    @Override
    public void userType(String type) {
        put("user_type", type);
    }

    @Override
    public void type(String type) {
        put("type", type);
    }

    @Override
    public void countryConfidence(int confidence) {
        put("country_confidence", confidence);
    }

    @Override
    public void cityConfidence(int confidence) {
        put("city_confidence", confidence);
    }

    @Override
    public void postalConfidence(int confidence) {
        put("postal_confidence", confidence);
    }

    @Override
    public void registeredCountryInEuropeanUnion(boolean val) {
        put("registered_country_in_european_union", val);
    }

    @Override
    public void registeredCountryIsoCode(String code) {
        put("registered_country_iso_code", code);
    }

    @Override
    public void registeredCountryName(String name) {
        put("registered_country_name", name);
    }

    @Override
    public void hosting(boolean val) {
        put("hosting", val);
    }

    @Override
    public void proxy(boolean val) {
        put("proxy", val);
    }

    @Override
    public void relay(boolean val) {
        put("relay", val);
    }

    @Override
    public void tor(boolean val) {
        put("tor", val);
    }

    @Override
    public void vpn(boolean val) {
        put("vpn", val);
    }

    @Override
    public void service(String service) {
        put("service", service);
    }
}
