/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.iplocation.api.IpDataLookupInfo;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of the public {@link org.elasticsearch.iplocation.api.IpDataLookup} interface.
 * Each instance is a "live handle" to a specific database: each {@link #lookup} call
 * obtains the latest database loader via reference counting.
 */
final class IpDataLookupImpl implements org.elasticsearch.iplocation.api.IpDataLookup {

    private final DatabaseNodeService databaseNodeService;
    private final ProjectId projectId;
    private final String databaseFile;
    private final String databaseType;
    private final IpDataLookup internalLookup;
    private final IpDataLookupInfo info;

    IpDataLookupImpl(
        DatabaseNodeService databaseNodeService,
        ProjectId projectId,
        String databaseFile,
        String databaseType,
        IpDataLookup internalLookup,
        IpDataLookupInfo info
    ) {
        this.databaseNodeService = databaseNodeService;
        this.projectId = projectId;
        this.databaseFile = databaseFile;
        this.databaseType = databaseType;
        this.internalLookup = internalLookup;
        this.info = info;
    }

    @Override
    public Boolean lookup(String ip, IpLocationInfoCollector collector) throws IOException {
        DatabaseReaderLazyLoader loader = databaseNodeService.getDatabaseReaderLazyLoader(projectId, databaseFile);
        if (loader == null) {
            return null;
        }
        try {
            if (Assertions.ENABLED) {
                verifyDatabaseType(loader);
            }
            Map<String, Object> data = internalLookup.getData(loader, ip);
            if (data.isEmpty()) {
                return false;
            }
            dispatchToCollector(collector, data);
            return true;
        } finally {
            loader.close();
        }
    }

    @Override
    public boolean isValid() {
        return databaseNodeService.isValid(projectId, databaseFile);
    }

    @Override
    public IpDataLookupInfo getInfo() {
        return info;
    }

    private void verifyDatabaseType(DatabaseReaderLazyLoader loader) throws IOException {
        int last = databaseType.lastIndexOf('-');
        final String expectedSuffix = last == -1 ? null : databaseType.substring(last);
        final String loaderType = loader.getDatabaseType();
        assert loaderType.equals(databaseType) || expectedSuffix == null || loaderType.endsWith(expectedSuffix)
            : "database type [" + loaderType + "] doesn't match with expected suffix [" + expectedSuffix + "]";
    }

    @SuppressWarnings("unchecked")
    static void dispatchToCollector(IpLocationInfoCollector collector, Map<String, Object> data) {
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            switch (entry.getKey()) {
                case "ip" -> collector.ip((String) entry.getValue());
                case "country_in_european_union" -> collector.countryInEuropeanUnion((Boolean) entry.getValue());
                case "country_iso_code" -> collector.countryIsoCode((String) entry.getValue());
                case "country_name" -> collector.countryName((String) entry.getValue());
                case "continent_code" -> collector.continentCode((String) entry.getValue());
                case "continent_name" -> collector.continentName((String) entry.getValue());
                case "region_iso_code" -> collector.regionIsoCode((String) entry.getValue());
                case "region_name" -> collector.regionName((String) entry.getValue());
                case "city_name" -> collector.cityName((String) entry.getValue());
                case "timezone" -> collector.timezone((String) entry.getValue());
                case "location" -> {
                    Map<String, Object> loc = (Map<String, Object>) entry.getValue();
                    collector.location((Double) loc.get("lat"), (Double) loc.get("lon"));
                }
                case "postal_code" -> collector.postalCode((String) entry.getValue());
                case "accuracy_radius" -> collector.accuracyRadius((Integer) entry.getValue());
                case "asn" -> collector.asn((Long) entry.getValue());
                case "organization_name" -> collector.organizationName((String) entry.getValue());
                case "network" -> collector.network((String) entry.getValue());
                case "hosting_provider" -> collector.hostingProvider((boolean) entry.getValue());
                case "tor_exit_node" -> collector.torExitNode((boolean) entry.getValue());
                case "anonymous_vpn" -> collector.anonymousVpn((boolean) entry.getValue());
                case "anonymous" -> collector.anonymous((boolean) entry.getValue());
                case "public_proxy" -> collector.publicProxy((boolean) entry.getValue());
                case "residential_proxy" -> collector.residentialProxy((boolean) entry.getValue());
                case "domain" -> collector.domain((String) entry.getValue());
                case "connection_type" -> collector.connectionType((String) entry.getValue());
                case "isp" -> collector.isp((String) entry.getValue());
                case "isp_organization_name" -> collector.ispOrganizationName((String) entry.getValue());
                case "mobile_country_code" -> collector.mobileCountryCode((String) entry.getValue());
                case "mobile_network_code" -> collector.mobileNetworkCode((String) entry.getValue());
                case "user_type" -> collector.userType((String) entry.getValue());
                case "type" -> collector.type((String) entry.getValue());
                case "country_confidence" -> collector.countryConfidence((Integer) entry.getValue());
                case "city_confidence" -> collector.cityConfidence((Integer) entry.getValue());
                case "postal_confidence" -> collector.postalConfidence((Integer) entry.getValue());
                case "registered_country_in_european_union" -> collector.registeredCountryInEuropeanUnion((Boolean) entry.getValue());
                case "registered_country_iso_code" -> collector.registeredCountryIsoCode((String) entry.getValue());
                case "registered_country_name" -> collector.registeredCountryName((String) entry.getValue());
                case "hosting" -> collector.hosting((Boolean) entry.getValue());
                case "proxy" -> collector.proxy((Boolean) entry.getValue());
                case "relay" -> collector.relay((Boolean) entry.getValue());
                case "tor" -> collector.tor((Boolean) entry.getValue());
                case "vpn" -> collector.vpn((Boolean) entry.getValue());
                case "service" -> collector.service((String) entry.getValue());
                default -> {
                }
            }
        }
    }
}
