/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.command;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.iplocation.api.DatabaseProperty;
import org.elasticsearch.iplocation.api.IpDataLookup;
import org.elasticsearch.iplocation.api.IpLocationInfoCollector;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.SequencedCollection;
import java.util.Set;

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
        /**
         * User-facing warning for lookup I/O failures. The raw {@link IOException} (database file/codec read errors,
         * etc.) is internal and not actionable for query users, so we surface this fixed message and log the real
         * exception at {@code DEBUG} in {@link #evaluate(String)} instead.
         */
        private final String ioFailureMessage;

        private final Map<Class<? extends Exception>, Set<String>> emittedWarningKeys = new HashMap<>();

        /**
         * Maps each {@link DatabaseProperty} to its output block index, or {@code -1} if that property was not requested.
         * Indexed by {@link DatabaseProperty#ordinal()}.
         */
        private final int[] fieldIndex;

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

            this.fieldIndex = new int[DatabaseProperty.values().length];
            Arrays.fill(fieldIndex, -1);
            this.keywordSlots = new boolean[outputFields.size()];

            int index = 0;
            for (String fieldName : outputFields) {
                try {
                    DatabaseProperty prop = DatabaseProperty.valueOf(fieldName.toUpperCase(Locale.ROOT));
                    fieldIndex[prop.ordinal()] = index;
                    if (prop.fieldType() == String.class) {
                        keywordSlots[index] = true;
                    }
                } catch (IllegalArgumentException e) {
                    // unrecognized field name — slot will be null-filled by endRow()
                }
                index++;
            }
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

        /**
         * {@code registerException} does not deduplicate (only the emitted HTTP header does, via
         * {@code HeaderWarning.addWarning}), and each call consumes one of the bounded {@code MAX_ADDED_WARNINGS} slots.
         * These failure messages can repeat per row, so we forward each distinct (exception type, message) only once:
         * this both avoids the per-call string building and synchronized {@code addWarning} (bounded to the first
         * {@code MAX_ADDED_WARNINGS} calls, after which {@code registerException} is a no-op) and keeps duplicates from
         * exhausting the budget and crowding out other distinct warnings. We use {@code registerException} rather than
         * the self-deduplicating {@code registerWarning} to keep the standard "evaluation failed, treating result as
         * null preamble and exception-class categorization shared by all other ES|QL failure warnings.
         */
        private void registerOnce(Class<? extends Exception> cls, String message) {
            if (emittedWarningKeys.computeIfAbsent(cls, k -> new HashSet<>()).add(message)) {
                warnings.registerException(cls, message);
            }
        }

        // --- Typed helpers: look up the output block index and write if the property was requested ---

        private void setString(DatabaseProperty prop, String value) {
            int idx = fieldIndex[prop.ordinal()];
            if (idx >= 0) {
                rowOutput.appendValue(value, idx);
            }
        }

        private void setBoolean(DatabaseProperty prop, boolean value) {
            int idx = fieldIndex[prop.ordinal()];
            if (idx >= 0) {
                rowOutput.appendValue(value, idx);
            }
        }

        private void setInt(DatabaseProperty prop, int value) {
            int idx = fieldIndex[prop.ordinal()];
            if (idx >= 0) {
                rowOutput.appendValue(value, idx);
            }
        }

        private void setLong(DatabaseProperty prop, long value) {
            int idx = fieldIndex[prop.ordinal()];
            if (idx >= 0) {
                rowOutput.appendValue(value, idx);
            }
        }

        private void setGeoPoint(DatabaseProperty prop, double lat, double lon) {
            int idx = fieldIndex[prop.ordinal()];
            if (idx >= 0) {
                rowOutput.appendGeoPoint(lat, lon, idx);
            }
        }

        // --- IpLocationInfoCollector method implementations ---

        @Override
        public void ip(String val) {
            setString(DatabaseProperty.IP, val);
        }

        @Override
        public void countryIsoCode(String code) {
            setString(DatabaseProperty.COUNTRY_ISO_CODE, code);
        }

        @Override
        public void countryName(String name) {
            setString(DatabaseProperty.COUNTRY_NAME, name);
        }

        @Override
        public void continentCode(String code) {
            setString(DatabaseProperty.CONTINENT_CODE, code);
        }

        @Override
        public void continentName(String name) {
            setString(DatabaseProperty.CONTINENT_NAME, name);
        }

        @Override
        public void regionIsoCode(String code) {
            setString(DatabaseProperty.REGION_ISO_CODE, code);
        }

        @Override
        public void regionName(String name) {
            setString(DatabaseProperty.REGION_NAME, name);
        }

        @Override
        public void cityName(String name) {
            setString(DatabaseProperty.CITY_NAME, name);
        }

        @Override
        public void timezone(String tz) {
            setString(DatabaseProperty.TIMEZONE, tz);
        }

        @Override
        public void postalCode(String code) {
            setString(DatabaseProperty.POSTAL_CODE, code);
        }

        @Override
        public void organizationName(String name) {
            setString(DatabaseProperty.ORGANIZATION_NAME, name);
        }

        @Override
        public void network(String val) {
            setString(DatabaseProperty.NETWORK, val);
        }

        @Override
        public void domain(String val) {
            setString(DatabaseProperty.DOMAIN, val);
        }

        @Override
        public void connectionType(String val) {
            setString(DatabaseProperty.CONNECTION_TYPE, val);
        }

        @Override
        public void isp(String val) {
            setString(DatabaseProperty.ISP, val);
        }

        @Override
        public void ispOrganizationName(String name) {
            setString(DatabaseProperty.ISP_ORGANIZATION_NAME, name);
        }

        @Override
        public void mobileCountryCode(String code) {
            setString(DatabaseProperty.MOBILE_COUNTRY_CODE, code);
        }

        @Override
        public void mobileNetworkCode(String code) {
            setString(DatabaseProperty.MOBILE_NETWORK_CODE, code);
        }

        @Override
        public void userType(String val) {
            setString(DatabaseProperty.USER_TYPE, val);
        }

        @Override
        public void type(String val) {
            setString(DatabaseProperty.TYPE, val);
        }

        @Override
        public void service(String val) {
            setString(DatabaseProperty.SERVICE, val);
        }

        @Override
        public void anycast(boolean val) {
            setBoolean(DatabaseProperty.ANYCAST, val);
        }

        @Override
        public void mobile(boolean val) {
            setBoolean(DatabaseProperty.MOBILE, val);
        }

        @Override
        public void satellite(boolean val) {
            setBoolean(DatabaseProperty.SATELLITE, val);
        }

        @Override
        public void dmaCode(String code) {
            setString(DatabaseProperty.DMA_CODE, code);
        }

        @Override
        public void geonameId(String id) {
            setString(DatabaseProperty.GEONAME_ID, id);
        }

        @Override
        public void asnChangedDate(String date) {
            setString(DatabaseProperty.ASN_CHANGED_DATE, date);
        }

        @Override
        public void geoChangedDate(String date) {
            setString(DatabaseProperty.GEO_CHANGED_DATE, date);
        }

        @Override
        public void registeredCountryIsoCode(String code) {
            setString(DatabaseProperty.REGISTERED_COUNTRY_ISO_CODE, code);
        }

        @Override
        public void registeredCountryName(String name) {
            setString(DatabaseProperty.REGISTERED_COUNTRY_NAME, name);
        }

        @Override
        public void countryInEuropeanUnion(boolean val) {
            setBoolean(DatabaseProperty.COUNTRY_IN_EUROPEAN_UNION, val);
        }

        @Override
        public void hostingProvider(boolean val) {
            setBoolean(DatabaseProperty.HOSTING_PROVIDER, val);
        }

        @Override
        public void torExitNode(boolean val) {
            setBoolean(DatabaseProperty.TOR_EXIT_NODE, val);
        }

        @Override
        public void anonymousVpn(boolean val) {
            setBoolean(DatabaseProperty.ANONYMOUS_VPN, val);
        }

        @Override
        public void anonymous(boolean val) {
            setBoolean(DatabaseProperty.ANONYMOUS, val);
        }

        @Override
        public void publicProxy(boolean val) {
            setBoolean(DatabaseProperty.PUBLIC_PROXY, val);
        }

        @Override
        public void residentialProxy(boolean val) {
            setBoolean(DatabaseProperty.RESIDENTIAL_PROXY, val);
        }

        @Override
        public void registeredCountryInEuropeanUnion(boolean val) {
            setBoolean(DatabaseProperty.REGISTERED_COUNTRY_IN_EUROPEAN_UNION, val);
        }

        @Override
        public void hosting(boolean val) {
            setBoolean(DatabaseProperty.HOSTING, val);
        }

        @Override
        public void proxy(boolean val) {
            setBoolean(DatabaseProperty.PROXY, val);
        }

        @Override
        public void relay(boolean val) {
            setBoolean(DatabaseProperty.RELAY, val);
        }

        @Override
        public void tor(boolean val) {
            setBoolean(DatabaseProperty.TOR, val);
        }

        @Override
        public void vpn(boolean val) {
            setBoolean(DatabaseProperty.VPN, val);
        }

        @Override
        public void accuracyRadius(int radius) {
            setInt(DatabaseProperty.ACCURACY_RADIUS, radius);
        }

        @Override
        public void countryConfidence(int confidence) {
            setInt(DatabaseProperty.COUNTRY_CONFIDENCE, confidence);
        }

        @Override
        public void cityConfidence(int confidence) {
            setInt(DatabaseProperty.CITY_CONFIDENCE, confidence);
        }

        @Override
        public void postalConfidence(int confidence) {
            setInt(DatabaseProperty.POSTAL_CONFIDENCE, confidence);
        }

        @Override
        public void asn(long asn) {
            setLong(DatabaseProperty.ASN, asn);
        }

        @Override
        public void location(double lat, double lon) {
            setGeoPoint(DatabaseProperty.LOCATION, lat, lon);
        }
    }
}
