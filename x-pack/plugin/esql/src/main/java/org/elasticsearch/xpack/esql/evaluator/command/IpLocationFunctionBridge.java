/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.ObjLongConsumer;

import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_BOOLEAN_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_GEO_POINT_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_INT_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_LONG_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.NOOP_STRING_COLLECTOR;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.booleanValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.geoPointValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.intValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.longValueCollector;
import static org.elasticsearch.xpack.esql.evaluator.command.CompoundOutputEvaluator.stringValueCollector;

/**
 * Bridge for the IP_LOCATION command that looks up IP addresses in geolocation databases.
 */
public final class IpLocationFunctionBridge {

    private IpLocationFunctionBridge() {}

    /**
     * Marker exception for the {@code Warnings.registerException(Class, String)} dedup key
     * when the IP location database is unavailable. Never thrown.
     */
    static final class IpLocationDatabaseUnavailableException extends Exception {
        IpLocationDatabaseUnavailableException() {}
    }

    /**
     * Marker exception for the {@code Warnings.registerException(Class, String)} dedup key
     * when the IP location database is expired. Never thrown.
     */
    static final class IpLocationDatabaseExpiredException extends Exception {
        IpLocationDatabaseExpiredException() {}
    }

    public static final class IpLocationCollectorImpl extends CompoundOutputEvaluator.OutputFieldsCollector
        implements
            IpLocationInfoCollector {

        private static final Logger logger = LogManager.getLogger(IpLocationCollectorImpl.class);
        private static final String EXPIRED_SENTINEL = "_ip_location_expired_database";

        @Nullable
        private final IpDataLookup ipDataLookup;
        private final boolean cachedIsValid;

        private final String unavailableSentinel;
        private final String unavailableMessage;
        private final String midQueryUnavailableMessage;
        private final String expiredMessage;
        private final String ioFailureMessage;

        private final Map<Class<? extends Exception>, Set<String>> emittedWarningKeys = new HashMap<>();

        // String collectors
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> ipCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> countryIsoCodeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> countryNameCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> continentCodeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> continentNameCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> regionIsoCodeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> regionNameCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> cityNameCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> timezoneCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> postalCodeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> organizationNameCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> networkCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> domainCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> connectionTypeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> ispCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> ispOrganizationNameCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> mobileCountryCodeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> mobileNetworkCodeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> userTypeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> typeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> serviceCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> registeredCountryIsoCodeCollector;
        private final BiConsumer<CompoundOutputEvaluator.RowOutput, String> registeredCountryNameCollector;

        // Boolean collectors
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> countryInEUCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> hostingProviderCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> torExitNodeCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> anonymousVpnCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> anonymousCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> publicProxyCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> residentialProxyCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> registeredCountryInEUCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> hostingCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> proxyCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> relayCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> torCollector;
        private final CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> vpnCollector;

        // Integer collectors
        private final ObjIntConsumer<CompoundOutputEvaluator.RowOutput> accuracyRadiusCollector;
        private final ObjIntConsumer<CompoundOutputEvaluator.RowOutput> countryConfidenceCollector;
        private final ObjIntConsumer<CompoundOutputEvaluator.RowOutput> cityConfidenceCollector;
        private final ObjIntConsumer<CompoundOutputEvaluator.RowOutput> postalConfidenceCollector;

        // Long collector
        private final ObjLongConsumer<CompoundOutputEvaluator.RowOutput> asnCollector;

        // Geo-point collector
        private final CompoundOutputEvaluator.GeoPointCollector locationCollector;

        /**
         * Tracks which output slots are KEYWORD-typed (String fields), for sentinel writing.
         */
        private final boolean[] keywordSlots;

        public IpLocationCollectorImpl(SequencedCollection<String> outputFields, @Nullable IpDataLookup ipDataLookup, String databaseFile) {
            super(outputFields.size());
            this.ipDataLookup = ipDataLookup;
            this.cachedIsValid = ipDataLookup != null && ipDataLookup.isValid();

            this.unavailableSentinel = "_ip_location_database_unavailable_" + databaseFile;
            this.unavailableMessage = "IP location database [" + databaseFile + "] is not available on this node";
            this.midQueryUnavailableMessage = "IP location database [" + databaseFile + "] became unavailable during query execution";
            this.expiredMessage = "IP location database [" + databaseFile + "] is expired";
            this.ioFailureMessage = "IP location lookup failed for database [" + databaseFile + "]";

            this.keywordSlots = new boolean[outputFields.size()];

            // Initialize all collectors to NOOP; the switch below wires the requested ones.
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> ip = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> countryIsoCode = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> countryName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> continentCode = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> continentName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> regionIsoCode = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> regionName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> cityName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> timezone = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> postalCode = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> organizationName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> network = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> domain = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> connectionType = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> isp = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> ispOrganizationName = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> mobileCountryCode = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> mobileNetworkCode = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> userType = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> type = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> service = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> registeredCountryIsoCode = NOOP_STRING_COLLECTOR;
            BiConsumer<CompoundOutputEvaluator.RowOutput, String> registeredCountryName = NOOP_STRING_COLLECTOR;

            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> countryInEU = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> hostingProvider = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> torExitNode = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> anonymousVpn = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> anonymous = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> publicProxy = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> residentialProxy = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> registeredCountryInEU = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> hosting = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> proxy = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> relay = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> tor = NOOP_BOOLEAN_COLLECTOR;
            CompoundOutputEvaluator.ObjBooleanConsumer<CompoundOutputEvaluator.RowOutput> vpn = NOOP_BOOLEAN_COLLECTOR;

            ObjIntConsumer<CompoundOutputEvaluator.RowOutput> accuracyRadius = NOOP_INT_COLLECTOR;
            ObjIntConsumer<CompoundOutputEvaluator.RowOutput> countryConfidence = NOOP_INT_COLLECTOR;
            ObjIntConsumer<CompoundOutputEvaluator.RowOutput> cityConfidence = NOOP_INT_COLLECTOR;
            ObjIntConsumer<CompoundOutputEvaluator.RowOutput> postalConfidence = NOOP_INT_COLLECTOR;

            ObjLongConsumer<CompoundOutputEvaluator.RowOutput> asn = NOOP_LONG_COLLECTOR;
            CompoundOutputEvaluator.GeoPointCollector location = NOOP_GEO_POINT_COLLECTOR;

            int index = 0;
            for (String fieldName : outputFields) {
                switch (fieldName) {
                    // String fields
                    case "ip" -> {
                        ip = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "country_iso_code" -> {
                        countryIsoCode = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "country_name" -> {
                        countryName = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "continent_code" -> {
                        continentCode = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "continent_name" -> {
                        continentName = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "region_iso_code" -> {
                        regionIsoCode = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "region_name" -> {
                        regionName = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "city_name" -> {
                        cityName = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "timezone" -> {
                        timezone = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "postal_code" -> {
                        postalCode = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "organization_name" -> {
                        organizationName = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "network" -> {
                        network = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "domain" -> {
                        domain = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "connection_type" -> {
                        connectionType = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "isp" -> {
                        isp = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "isp_organization_name" -> {
                        ispOrganizationName = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "mobile_country_code" -> {
                        mobileCountryCode = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "mobile_network_code" -> {
                        mobileNetworkCode = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "user_type" -> {
                        userType = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "type" -> {
                        type = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "service" -> {
                        service = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "registered_country_iso_code" -> {
                        registeredCountryIsoCode = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    case "registered_country_name" -> {
                        registeredCountryName = stringValueCollector(index);
                        keywordSlots[index] = true;
                    }
                    // Boolean fields
                    case "country_in_european_union" -> countryInEU = booleanValueCollector(index);
                    case "hosting_provider" -> hostingProvider = booleanValueCollector(index);
                    case "tor_exit_node" -> torExitNode = booleanValueCollector(index);
                    case "anonymous_vpn" -> anonymousVpn = booleanValueCollector(index);
                    case "anonymous" -> anonymous = booleanValueCollector(index);
                    case "public_proxy" -> publicProxy = booleanValueCollector(index);
                    case "residential_proxy" -> residentialProxy = booleanValueCollector(index);
                    case "registered_country_in_european_union" -> registeredCountryInEU = booleanValueCollector(index);
                    case "hosting" -> hosting = booleanValueCollector(index);
                    case "proxy" -> proxy = booleanValueCollector(index);
                    case "relay" -> relay = booleanValueCollector(index);
                    case "tor" -> tor = booleanValueCollector(index);
                    case "vpn" -> vpn = booleanValueCollector(index);
                    // Integer fields
                    case "accuracy_radius" -> accuracyRadius = intValueCollector(index, v -> true);
                    case "country_confidence" -> countryConfidence = intValueCollector(index, v -> true);
                    case "city_confidence" -> cityConfidence = intValueCollector(index, v -> true);
                    case "postal_confidence" -> postalConfidence = intValueCollector(index, v -> true);
                    // Long field
                    case "asn" -> asn = longValueCollector(index);
                    // Geo-point field
                    case "location" -> location = geoPointValueCollector(index);
                    default -> {
                        // unknown field — corresponding block will be filled with nulls
                    }
                }
                index++;
            }

            this.ipCollector = ip;
            this.countryIsoCodeCollector = countryIsoCode;
            this.countryNameCollector = countryName;
            this.continentCodeCollector = continentCode;
            this.continentNameCollector = continentName;
            this.regionIsoCodeCollector = regionIsoCode;
            this.regionNameCollector = regionName;
            this.cityNameCollector = cityName;
            this.timezoneCollector = timezone;
            this.postalCodeCollector = postalCode;
            this.organizationNameCollector = organizationName;
            this.networkCollector = network;
            this.domainCollector = domain;
            this.connectionTypeCollector = connectionType;
            this.ispCollector = isp;
            this.ispOrganizationNameCollector = ispOrganizationName;
            this.mobileCountryCodeCollector = mobileCountryCode;
            this.mobileNetworkCodeCollector = mobileNetworkCode;
            this.userTypeCollector = userType;
            this.typeCollector = type;
            this.serviceCollector = service;
            this.registeredCountryIsoCodeCollector = registeredCountryIsoCode;
            this.registeredCountryNameCollector = registeredCountryName;

            this.countryInEUCollector = countryInEU;
            this.hostingProviderCollector = hostingProvider;
            this.torExitNodeCollector = torExitNode;
            this.anonymousVpnCollector = anonymousVpn;
            this.anonymousCollector = anonymous;
            this.publicProxyCollector = publicProxy;
            this.residentialProxyCollector = residentialProxy;
            this.registeredCountryInEUCollector = registeredCountryInEU;
            this.hostingCollector = hosting;
            this.proxyCollector = proxy;
            this.relayCollector = relay;
            this.torCollector = tor;
            this.vpnCollector = vpn;

            this.accuracyRadiusCollector = accuracyRadius;
            this.countryConfidenceCollector = countryConfidence;
            this.cityConfidenceCollector = cityConfidence;
            this.postalConfidenceCollector = postalConfidence;

            this.asnCollector = asn;
            this.locationCollector = location;
        }

        @Override
        protected void evaluate(String input) {
            if (ipDataLookup == null) {
                fillSentinelToKeywordFields(unavailableSentinel);
                registerOnce(IpLocationDatabaseUnavailableException.class, unavailableMessage);
                return;
            }
            if (cachedIsValid == false) {
                fillSentinelToKeywordFields(EXPIRED_SENTINEL);
                registerOnce(IpLocationDatabaseExpiredException.class, expiredMessage);
                return;
            }
            Boolean result;
            try {
                result = ipDataLookup.lookup(input, this);
            } catch (IOException e) {
                registerOnce(IOException.class, ioFailureMessage);
                if (logger.isDebugEnabled()) {
                    logger.debug(() -> "IP location lookup failed for [" + input + "] with [" + e.getClass().getSimpleName() + "]", e);
                }
                return;
            }
            if (result == null) {
                fillSentinelToKeywordFields(unavailableSentinel);
                registerOnce(IpLocationDatabaseUnavailableException.class, midQueryUnavailableMessage);
            }
            // result==false → no callbacks invoked, fillMissingValues nullifies
            // result==true → callbacks already pushed values
        }

        private void fillSentinelToKeywordFields(String sentinel) {
            for (int i = 0; i < keywordSlots.length; i++) {
                if (keywordSlots[i]) {
                    rowOutput.appendValue(sentinel, i);
                }
            }
        }

        private void registerOnce(Class<? extends Exception> cls, String message) {
            if (emittedWarningKeys.computeIfAbsent(cls, k -> new HashSet<>()).add(message)) {
                warnings.registerException(cls, message);
            }
        }

        // --- IpLocationInfoCollector method implementations ---

        @Override
        public void ip(String val) {
            ipCollector.accept(rowOutput, val);
        }

        @Override
        public void countryIsoCode(String code) {
            countryIsoCodeCollector.accept(rowOutput, code);
        }

        @Override
        public void countryName(String name) {
            countryNameCollector.accept(rowOutput, name);
        }

        @Override
        public void continentCode(String code) {
            continentCodeCollector.accept(rowOutput, code);
        }

        @Override
        public void continentName(String name) {
            continentNameCollector.accept(rowOutput, name);
        }

        @Override
        public void regionIsoCode(String code) {
            regionIsoCodeCollector.accept(rowOutput, code);
        }

        @Override
        public void regionName(String name) {
            regionNameCollector.accept(rowOutput, name);
        }

        @Override
        public void cityName(String name) {
            cityNameCollector.accept(rowOutput, name);
        }

        @Override
        public void timezone(String tz) {
            timezoneCollector.accept(rowOutput, tz);
        }

        @Override
        public void postalCode(String code) {
            postalCodeCollector.accept(rowOutput, code);
        }

        @Override
        public void organizationName(String name) {
            organizationNameCollector.accept(rowOutput, name);
        }

        @Override
        public void network(String val) {
            networkCollector.accept(rowOutput, val);
        }

        @Override
        public void domain(String val) {
            domainCollector.accept(rowOutput, val);
        }

        @Override
        public void connectionType(String val) {
            connectionTypeCollector.accept(rowOutput, val);
        }

        @Override
        public void isp(String val) {
            ispCollector.accept(rowOutput, val);
        }

        @Override
        public void ispOrganizationName(String name) {
            ispOrganizationNameCollector.accept(rowOutput, name);
        }

        @Override
        public void mobileCountryCode(String code) {
            mobileCountryCodeCollector.accept(rowOutput, code);
        }

        @Override
        public void mobileNetworkCode(String code) {
            mobileNetworkCodeCollector.accept(rowOutput, code);
        }

        @Override
        public void userType(String val) {
            userTypeCollector.accept(rowOutput, val);
        }

        @Override
        public void type(String val) {
            typeCollector.accept(rowOutput, val);
        }

        @Override
        public void service(String val) {
            serviceCollector.accept(rowOutput, val);
        }

        @Override
        public void registeredCountryIsoCode(String code) {
            registeredCountryIsoCodeCollector.accept(rowOutput, code);
        }

        @Override
        public void registeredCountryName(String name) {
            registeredCountryNameCollector.accept(rowOutput, name);
        }

        @Override
        public void countryInEuropeanUnion(boolean val) {
            countryInEUCollector.accept(rowOutput, val);
        }

        @Override
        public void hostingProvider(boolean val) {
            hostingProviderCollector.accept(rowOutput, val);
        }

        @Override
        public void torExitNode(boolean val) {
            torExitNodeCollector.accept(rowOutput, val);
        }

        @Override
        public void anonymousVpn(boolean val) {
            anonymousVpnCollector.accept(rowOutput, val);
        }

        @Override
        public void anonymous(boolean val) {
            anonymousCollector.accept(rowOutput, val);
        }

        @Override
        public void publicProxy(boolean val) {
            publicProxyCollector.accept(rowOutput, val);
        }

        @Override
        public void residentialProxy(boolean val) {
            residentialProxyCollector.accept(rowOutput, val);
        }

        @Override
        public void registeredCountryInEuropeanUnion(boolean val) {
            registeredCountryInEUCollector.accept(rowOutput, val);
        }

        @Override
        public void hosting(boolean val) {
            hostingCollector.accept(rowOutput, val);
        }

        @Override
        public void proxy(boolean val) {
            proxyCollector.accept(rowOutput, val);
        }

        @Override
        public void relay(boolean val) {
            relayCollector.accept(rowOutput, val);
        }

        @Override
        public void tor(boolean val) {
            torCollector.accept(rowOutput, val);
        }

        @Override
        public void vpn(boolean val) {
            vpnCollector.accept(rowOutput, val);
        }

        @Override
        public void accuracyRadius(int radius) {
            accuracyRadiusCollector.accept(rowOutput, radius);
        }

        @Override
        public void countryConfidence(int confidence) {
            countryConfidenceCollector.accept(rowOutput, confidence);
        }

        @Override
        public void cityConfidence(int confidence) {
            cityConfidenceCollector.accept(rowOutput, confidence);
        }

        @Override
        public void postalConfidence(int confidence) {
            postalConfidenceCollector.accept(rowOutput, confidence);
        }

        @Override
        public void asn(long asn) {
            asnCollector.accept(rowOutput, asn);
        }

        @Override
        public void location(double lat, double lon) {
            locationCollector.accept(rowOutput, lat, lon);
        }
    }
}
