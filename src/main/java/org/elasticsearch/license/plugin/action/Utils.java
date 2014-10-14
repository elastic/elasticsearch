/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.action;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.elasticsearch.license.plugin.core.TrialLicenses;
import org.elasticsearch.license.plugin.core.TrialLicensesBuilder;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.license.core.ESLicenses.*;
import static org.elasticsearch.license.manager.Utils.getESLicenseFromSignature;
import static org.elasticsearch.license.plugin.core.TrialLicenses.TrialLicense;
import static org.elasticsearch.license.plugin.core.TrialLicensesBuilder.fromEncodedTrialLicense;
import static org.elasticsearch.license.plugin.core.TrialLicensesBuilder.toEncodedTrialLicense;
import static org.elasticsearch.license.plugin.core.TrialLicensesBuilder.trialLicensesBuilder;

public class Utils {

    public static ESLicenses readGeneratedLicensesFromMetaData(StreamInput in) throws IOException {
        boolean exists = in.readBoolean();
        return exists ? fromSignatures(in.readStringArray()) : null;
    }

    public static void writeGeneratedLicensesToMetaData(ESLicenses esLicenses, StreamOutput out) throws IOException {
        if (esLicenses == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeStringArray(toSignatures(esLicenses));
        }
    }

    public static TrialLicenses readTrialLicensesFromMetaData(StreamInput in) throws IOException {
        boolean exists = in.readBoolean();
        return exists ? fromEncodedTrialLicenses(in.readStringArray()) : null;

    }

    public static void writeTrialLicensesToMetaData(TrialLicenses trialLicenses, StreamOutput out) throws IOException {
        if (trialLicenses == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeStringArray(toEncodedTrialLicenses(trialLicenses));
        }
    }

    public static String[] toEncodedTrialLicenses(TrialLicenses trialLicenses) {
        Set<String> encodedTrialLicenses = new HashSet<>();
        for (TrialLicense trialLicense : trialLicenses) {
            encodedTrialLicenses.add(toEncodedTrialLicense(trialLicense));
        }
        return encodedTrialLicenses.toArray(new String[encodedTrialLicenses.size()]);
    }

    public static TrialLicenses fromEncodedTrialLicenses(String[] encodedTrialLicenses) {
        final TrialLicensesBuilder trialLicensesBuilder = trialLicensesBuilder();
        for (String encodedTrialLicense : encodedTrialLicenses) {
            trialLicensesBuilder.license(fromEncodedTrialLicense(encodedTrialLicense));
        }
        return trialLicensesBuilder.build();
    }

    public static TrialLicenses fromEncodedTrialLicenses(Set<String> encodedTrialLicenses) {
        return fromEncodedTrialLicenses(encodedTrialLicenses.toArray(new String[encodedTrialLicenses.size()]));
    }

    public static String[] toSignatures(ESLicenses esLicenses) {
        Set<String> signatures = new HashSet<>();
        for (ESLicense esLicense : esLicenses) {
            signatures.add(esLicense.signature());
        }
        return signatures.toArray(new String[signatures.size()]);
    }

    public static ESLicenses fromSignatures(final String[] signatures) {
        return fromSignatures(Sets.newHashSet(signatures));
    }

    public static ESLicenses fromSignatures(final Set<String> signatures) {
        final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
        for (String signature : signatures) {
            licensesBuilder.license(getESLicenseFromSignature(signature));
        }
        return licensesBuilder.build();
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

    public static TrialLicenses readTrialLicensesFrom(StreamInput in) throws IOException {
        final TrialLicensesBuilder licensesBuilder = TrialLicensesBuilder.trialLicensesBuilder();
        boolean exists = in.readBoolean();
        if (exists) {
            int size = in.readVInt();
            for (int i = 0; i < size; i++) {
                licensesBuilder.license(trialLicenseFromMap(in.readMap()));
            }
            return licensesBuilder.build();
        }
        return null;
    }

    public static void writeTrialLicensesTo(TrialLicenses trialLicenses, StreamOutput out) throws IOException {
        if (trialLicenses == null) {
            out.writeBoolean(false);
            return;
        }
        out.writeBoolean(true);
        out.writeVInt(trialLicenses.trialLicenses().size());
        for (TrialLicense trialLicense : trialLicenses) {
            out.writeMap(trialLicenseAsMap(trialLicense));
        }
    }

    // TODO: make sure field order is preserved
    public static Map<String, Object> trialLicenseAsMap(TrialLicense trialLicense) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put(TrialLicenseFields.UID, trialLicense.uid());
        builder.put(TrialLicenseFields.TYPE, Type.TRIAL.string());
        builder.put(TrialLicenseFields.SUBSCRIPTION_TYPE, SubscriptionType.NONE.string());
        builder.put(TrialLicenseFields.ISSUE_DATE, trialLicense.issueDate());
        builder.put(TrialLicenseFields.FEATURE, trialLicense.feature().string());
        builder.put(TrialLicenseFields.EXPIRY_DATE, trialLicense.expiryDate());
        builder.put(TrialLicenseFields.MAX_NODES, trialLicense.maxNodes());
        builder.put(TrialLicenseFields.ISSUED_TO, trialLicense.issuedTo());
        return builder.build();
    }

    public static TrialLicense trialLicenseFromMap(Map<String, Object> map) {
        return TrialLicensesBuilder.trialLicenseBuilder()
                .uid((String) map.get(TrialLicenseFields.UID))
                .issuedTo((String) map.get(TrialLicenseFields.ISSUED_TO))
                .maxNodes((int) map.get(TrialLicenseFields.MAX_NODES))
                .feature(FeatureType.fromString((String) map.get(TrialLicenseFields.FEATURE)))
                .issueDate((long) map.get(TrialLicenseFields.ISSUE_DATE))
                .expiryDate((long) map.get(TrialLicenseFields.EXPIRY_DATE))
                .build();

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

    final static class TrialLicenseFields {
        private final static String UID = "uid";
        private final static String TYPE = "type";
        private final static String SUBSCRIPTION_TYPE = "subscription_type";
        private final static String ISSUE_DATE = "issue_date";
        private final static String FEATURE = "feature";
        private final static String ISSUED_TO = "issued_to";
        private final static String MAX_NODES = "max_nodes";
        private final static String EXPIRY_DATE = "expiry_date";
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
