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
public final class IpLocationInfoMapCollector extends HashMap<String, Object> implements IpLocationInfoCollector {

    @Override
    public void ip(String ip) {
        put(DatabaseProperty.IP.fieldName(), ip);
    }

    @Override
    public void countryInEuropeanUnion(boolean val) {
        put(DatabaseProperty.COUNTRY_IN_EUROPEAN_UNION.fieldName(), val);
    }

    @Override
    public void countryIsoCode(String code) {
        put(DatabaseProperty.COUNTRY_ISO_CODE.fieldName(), code);
    }

    @Override
    public void countryName(String name) {
        put(DatabaseProperty.COUNTRY_NAME.fieldName(), name);
    }

    @Override
    public void continentCode(String code) {
        put(DatabaseProperty.CONTINENT_CODE.fieldName(), code);
    }

    @Override
    public void continentName(String name) {
        put(DatabaseProperty.CONTINENT_NAME.fieldName(), name);
    }

    @Override
    public void regionIsoCode(String code) {
        put(DatabaseProperty.REGION_ISO_CODE.fieldName(), code);
    }

    @Override
    public void regionName(String name) {
        put(DatabaseProperty.REGION_NAME.fieldName(), name);
    }

    @Override
    public void cityName(String name) {
        put(DatabaseProperty.CITY_NAME.fieldName(), name);
    }

    @Override
    public void timezone(String tz) {
        put(DatabaseProperty.TIMEZONE.fieldName(), tz);
    }

    @Override
    public void location(double lat, double lon) {
        put(DatabaseProperty.LOCATION.fieldName(), Map.of("lat", lat, "lon", lon));
    }

    @Override
    public void postalCode(String code) {
        put(DatabaseProperty.POSTAL_CODE.fieldName(), code);
    }

    @Override
    public void accuracyRadius(int radius) {
        put(DatabaseProperty.ACCURACY_RADIUS.fieldName(), radius);
    }

    @Override
    public void asn(long asn) {
        put(DatabaseProperty.ASN.fieldName(), asn);
    }

    @Override
    public void organizationName(String name) {
        put(DatabaseProperty.ORGANIZATION_NAME.fieldName(), name);
    }

    @Override
    public void network(String network) {
        put(DatabaseProperty.NETWORK.fieldName(), network);
    }

    @Override
    public void hostingProvider(boolean val) {
        put(DatabaseProperty.HOSTING_PROVIDER.fieldName(), val);
    }

    @Override
    public void torExitNode(boolean val) {
        put(DatabaseProperty.TOR_EXIT_NODE.fieldName(), val);
    }

    @Override
    public void anonymousVpn(boolean val) {
        put(DatabaseProperty.ANONYMOUS_VPN.fieldName(), val);
    }

    @Override
    public void anonymous(boolean val) {
        put(DatabaseProperty.ANONYMOUS.fieldName(), val);
    }

    @Override
    public void publicProxy(boolean val) {
        put(DatabaseProperty.PUBLIC_PROXY.fieldName(), val);
    }

    @Override
    public void residentialProxy(boolean val) {
        put(DatabaseProperty.RESIDENTIAL_PROXY.fieldName(), val);
    }

    @Override
    public void domain(String domain) {
        put(DatabaseProperty.DOMAIN.fieldName(), domain);
    }

    @Override
    public void connectionType(String type) {
        put(DatabaseProperty.CONNECTION_TYPE.fieldName(), type);
    }

    @Override
    public void isp(String isp) {
        put(DatabaseProperty.ISP.fieldName(), isp);
    }

    @Override
    public void ispOrganizationName(String name) {
        put(DatabaseProperty.ISP_ORGANIZATION_NAME.fieldName(), name);
    }

    @Override
    public void mobileCountryCode(String code) {
        put(DatabaseProperty.MOBILE_COUNTRY_CODE.fieldName(), code);
    }

    @Override
    public void mobileNetworkCode(String code) {
        put(DatabaseProperty.MOBILE_NETWORK_CODE.fieldName(), code);
    }

    @Override
    public void userType(String type) {
        put(DatabaseProperty.USER_TYPE.fieldName(), type);
    }

    @Override
    public void type(String type) {
        put(DatabaseProperty.TYPE.fieldName(), type);
    }

    @Override
    public void countryConfidence(int confidence) {
        put(DatabaseProperty.COUNTRY_CONFIDENCE.fieldName(), confidence);
    }

    @Override
    public void cityConfidence(int confidence) {
        put(DatabaseProperty.CITY_CONFIDENCE.fieldName(), confidence);
    }

    @Override
    public void postalConfidence(int confidence) {
        put(DatabaseProperty.POSTAL_CONFIDENCE.fieldName(), confidence);
    }

    @Override
    public void registeredCountryInEuropeanUnion(boolean val) {
        put(DatabaseProperty.REGISTERED_COUNTRY_IN_EUROPEAN_UNION.fieldName(), val);
    }

    @Override
    public void registeredCountryIsoCode(String code) {
        put(DatabaseProperty.REGISTERED_COUNTRY_ISO_CODE.fieldName(), code);
    }

    @Override
    public void registeredCountryName(String name) {
        put(DatabaseProperty.REGISTERED_COUNTRY_NAME.fieldName(), name);
    }

    @Override
    public void hosting(boolean val) {
        put(DatabaseProperty.HOSTING.fieldName(), val);
    }

    @Override
    public void proxy(boolean val) {
        put(DatabaseProperty.PROXY.fieldName(), val);
    }

    @Override
    public void relay(boolean val) {
        put(DatabaseProperty.RELAY.fieldName(), val);
    }

    @Override
    public void tor(boolean val) {
        put(DatabaseProperty.TOR.fieldName(), val);
    }

    @Override
    public void vpn(boolean val) {
        put(DatabaseProperty.VPN.fieldName(), val);
    }

    @Override
    public void service(String service) {
        put(DatabaseProperty.SERVICE.fieldName(), service);
    }

    @Override
    public void anycast(boolean val) {
        put(DatabaseProperty.ANYCAST.fieldName(), val);
    }

    @Override
    public void mobile(boolean val) {
        put(DatabaseProperty.MOBILE.fieldName(), val);
    }

    @Override
    public void satellite(boolean val) {
        put(DatabaseProperty.SATELLITE.fieldName(), val);
    }

    @Override
    public void dmaCode(String code) {
        put(DatabaseProperty.DMA_CODE.fieldName(), code);
    }

    @Override
    public void geonameId(String id) {
        put(DatabaseProperty.GEONAME_ID.fieldName(), id);
    }

    @Override
    public void asnChangedDate(String date) {
        put(DatabaseProperty.ASN_CHANGED_DATE.fieldName(), date);
    }

    @Override
    public void geoChangedDate(String date) {
        put(DatabaseProperty.GEO_CHANGED_DATE.fieldName(), date);
    }
}
