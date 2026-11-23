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
public final class IpLocationInfoMapCollector implements IpLocationInfoCollector {

    private final Map<String, Object> map;

    /**
     * @param map the map to put the collected values into; the caller keeps the reference and reads the results from it
     */
    public IpLocationInfoMapCollector(Map<String, Object> map) {
        this.map = map;
    }

    @Override
    public void ip(String ip) {
        map.put(DatabaseProperty.IP.fieldName(), ip);
    }

    @Override
    public void countryInEuropeanUnion(boolean val) {
        map.put(DatabaseProperty.COUNTRY_IN_EUROPEAN_UNION.fieldName(), val);
    }

    @Override
    public void countryIsoCode(String code) {
        map.put(DatabaseProperty.COUNTRY_ISO_CODE.fieldName(), code);
    }

    @Override
    public void countryName(String name) {
        map.put(DatabaseProperty.COUNTRY_NAME.fieldName(), name);
    }

    @Override
    public void continentCode(String code) {
        map.put(DatabaseProperty.CONTINENT_CODE.fieldName(), code);
    }

    @Override
    public void continentName(String name) {
        map.put(DatabaseProperty.CONTINENT_NAME.fieldName(), name);
    }

    @Override
    public void regionIsoCode(String code) {
        map.put(DatabaseProperty.REGION_ISO_CODE.fieldName(), code);
    }

    @Override
    public void regionName(String name) {
        map.put(DatabaseProperty.REGION_NAME.fieldName(), name);
    }

    @Override
    public void cityName(String name) {
        map.put(DatabaseProperty.CITY_NAME.fieldName(), name);
    }

    @Override
    public void timezone(String tz) {
        map.put(DatabaseProperty.TIMEZONE.fieldName(), tz);
    }

    @Override
    public void location(double lat, double lon) {
        Map<String, Object> locationObject = HashMap.newHashMap(2); // note: we need to return a mutable object here
        locationObject.put("lat", lat);
        locationObject.put("lon", lon);
        map.put(DatabaseProperty.LOCATION.fieldName(), locationObject);
    }

    @Override
    public void postalCode(String code) {
        map.put(DatabaseProperty.POSTAL_CODE.fieldName(), code);
    }

    @Override
    public void accuracyRadius(int radius) {
        map.put(DatabaseProperty.ACCURACY_RADIUS.fieldName(), radius);
    }

    @Override
    public void asn(long asn) {
        map.put(DatabaseProperty.ASN.fieldName(), asn);
    }

    @Override
    public void organizationName(String name) {
        map.put(DatabaseProperty.ORGANIZATION_NAME.fieldName(), name);
    }

    @Override
    public void network(String network) {
        map.put(DatabaseProperty.NETWORK.fieldName(), network);
    }

    @Override
    public void hostingProvider(boolean val) {
        map.put(DatabaseProperty.HOSTING_PROVIDER.fieldName(), val);
    }

    @Override
    public void torExitNode(boolean val) {
        map.put(DatabaseProperty.TOR_EXIT_NODE.fieldName(), val);
    }

    @Override
    public void anonymousVpn(boolean val) {
        map.put(DatabaseProperty.ANONYMOUS_VPN.fieldName(), val);
    }

    @Override
    public void anonymous(boolean val) {
        map.put(DatabaseProperty.ANONYMOUS.fieldName(), val);
    }

    @Override
    public void publicProxy(boolean val) {
        map.put(DatabaseProperty.PUBLIC_PROXY.fieldName(), val);
    }

    @Override
    public void residentialProxy(boolean val) {
        map.put(DatabaseProperty.RESIDENTIAL_PROXY.fieldName(), val);
    }

    @Override
    public void domain(String domain) {
        map.put(DatabaseProperty.DOMAIN.fieldName(), domain);
    }

    @Override
    public void connectionType(String type) {
        map.put(DatabaseProperty.CONNECTION_TYPE.fieldName(), type);
    }

    @Override
    public void isp(String isp) {
        map.put(DatabaseProperty.ISP.fieldName(), isp);
    }

    @Override
    public void ispOrganizationName(String name) {
        map.put(DatabaseProperty.ISP_ORGANIZATION_NAME.fieldName(), name);
    }

    @Override
    public void mobileCountryCode(String code) {
        map.put(DatabaseProperty.MOBILE_COUNTRY_CODE.fieldName(), code);
    }

    @Override
    public void mobileNetworkCode(String code) {
        map.put(DatabaseProperty.MOBILE_NETWORK_CODE.fieldName(), code);
    }

    @Override
    public void userType(String type) {
        map.put(DatabaseProperty.USER_TYPE.fieldName(), type);
    }

    @Override
    public void type(String type) {
        map.put(DatabaseProperty.TYPE.fieldName(), type);
    }

    @Override
    public void countryConfidence(int confidence) {
        map.put(DatabaseProperty.COUNTRY_CONFIDENCE.fieldName(), confidence);
    }

    @Override
    public void cityConfidence(int confidence) {
        map.put(DatabaseProperty.CITY_CONFIDENCE.fieldName(), confidence);
    }

    @Override
    public void postalConfidence(int confidence) {
        map.put(DatabaseProperty.POSTAL_CONFIDENCE.fieldName(), confidence);
    }

    @Override
    public void registeredCountryInEuropeanUnion(boolean val) {
        map.put(DatabaseProperty.REGISTERED_COUNTRY_IN_EUROPEAN_UNION.fieldName(), val);
    }

    @Override
    public void registeredCountryIsoCode(String code) {
        map.put(DatabaseProperty.REGISTERED_COUNTRY_ISO_CODE.fieldName(), code);
    }

    @Override
    public void registeredCountryName(String name) {
        map.put(DatabaseProperty.REGISTERED_COUNTRY_NAME.fieldName(), name);
    }

    @Override
    public void hosting(boolean val) {
        map.put(DatabaseProperty.HOSTING.fieldName(), val);
    }

    @Override
    public void proxy(boolean val) {
        map.put(DatabaseProperty.PROXY.fieldName(), val);
    }

    @Override
    public void relay(boolean val) {
        map.put(DatabaseProperty.RELAY.fieldName(), val);
    }

    @Override
    public void tor(boolean val) {
        map.put(DatabaseProperty.TOR.fieldName(), val);
    }

    @Override
    public void vpn(boolean val) {
        map.put(DatabaseProperty.VPN.fieldName(), val);
    }

    @Override
    public void service(String service) {
        map.put(DatabaseProperty.SERVICE.fieldName(), service);
    }

    @Override
    public void anycast(boolean val) {
        map.put(DatabaseProperty.ANYCAST.fieldName(), val);
    }

    @Override
    public void mobile(boolean val) {
        map.put(DatabaseProperty.MOBILE.fieldName(), val);
    }

    @Override
    public void satellite(boolean val) {
        map.put(DatabaseProperty.SATELLITE.fieldName(), val);
    }

    @Override
    public void dmaCode(String code) {
        map.put(DatabaseProperty.DMA_CODE.fieldName(), code);
    }

    @Override
    public void geonameId(String id) {
        map.put(DatabaseProperty.GEONAME_ID.fieldName(), id);
    }

    @Override
    public void asnChangedDate(String date) {
        map.put(DatabaseProperty.ASN_CHANGED_DATE.fieldName(), date);
    }

    @Override
    public void geoChangedDate(String date) {
        map.put(DatabaseProperty.GEO_CHANGED_DATE.fieldName(), date);
    }
}
