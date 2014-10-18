/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.license.core.ESLicenses.*;

public class Utils {

    public static String[] toSignatures(ESLicenses esLicenses) {
        Set<String> signatures = new HashSet<>();
        for (ESLicense esLicense : esLicenses) {
            signatures.add(esLicense.signature());
        }
        return signatures.toArray(new String[signatures.size()]);
    }

    public static ESLicenses readGeneratedLicensesFrom(StreamInput in) throws IOException {
        final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
        boolean exists = in.readBoolean();
        if (exists) {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                licensesBuilder.licenseAsIs(licenseFromMap(in.readMap()));
            }
            return licensesBuilder.build();
        }
        return null;
    }

    public static void writeGeneratedLicensesTo(ESLicenses esLicenses, StreamOutput out) throws IOException {
        if (esLicenses == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeVInt(esLicenses.licenses().size());
        for (ESLicense esLicense : esLicenses) {
            out.writeMap(licenseAsMap(esLicense));
        }
    }

    // TODO: make sure field order is preserved
    public static Map<String, Object> licenseAsMap(ESLicense esLicense) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put(LicenseFields.UID, esLicense.uid());
        builder.put(LicenseFields.TYPE, esLicense.type().string());
        builder.put(LicenseFields.SUBSCRIPTION_TYPE, esLicense.subscriptionType().string());
        builder.put(LicenseFields.ISSUE_DATE, esLicense.issueDate());
        builder.put(LicenseFields.FEATURE, esLicense.feature().string());
        builder.put(LicenseFields.EXPIRY_DATE, esLicense.expiryDate());
        builder.put(LicenseFields.MAX_NODES, esLicense.maxNodes());
        builder.put(LicenseFields.ISSUED_TO, esLicense.issuedTo());
        builder.put(LicenseFields.SIGNATURE, esLicense.signature());
        return builder.build();
    }

    public static ESLicense licenseFromMap(Map<String, Object> map) {
        return LicenseBuilders.licenseBuilder(false)
                .uid((String) map.get(LicenseFields.UID))
                .type(Type.fromString((String) map.get(LicenseFields.TYPE)))
                .subscriptionType(SubscriptionType.fromString((String) map.get(LicenseFields.SUBSCRIPTION_TYPE)))
                .issueDate((long) map.get(LicenseFields.ISSUE_DATE))
                .feature(FeatureType.fromString((String) map.get(LicenseFields.FEATURE)))
                .expiryDate((long) map.get(LicenseFields.EXPIRY_DATE))
                .maxNodes((int) map.get(LicenseFields.MAX_NODES))
                .issuedTo((String) map.get(LicenseFields.ISSUED_TO))
                .signature((String) map.get(LicenseFields.SIGNATURE))
                .build();

    }

    final static class LicenseFields {
        private final static String UID = "uid";
        private final static String TYPE = "type";
        private final static String SUBSCRIPTION_TYPE = "subscription_type";
        private final static String ISSUE_DATE = "issue_date";
        private final static String FEATURE = "feature";
        private final static String EXPIRY_DATE = "expiry_date";
        private final static String MAX_NODES = "max_nodes";
        private final static String ISSUED_TO = "issued_to";
        private final static String SIGNATURE = "signature";
    }
}
