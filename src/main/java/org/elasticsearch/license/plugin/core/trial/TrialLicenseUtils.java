/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core.trial;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.core.ESLicenses;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.license.plugin.core.trial.TrialLicensesBuilder.trialLicensesBuilder;

public class TrialLicenseUtils {

    public static TrialLicenses.TrialLicense fromEncodedTrialLicense(String encodedTrialLicense) {
        byte[] encodedBytes = Base64.decodeBase64(encodedTrialLicense);
        ByteBuffer byteBuffer = ByteBuffer.wrap(encodedBytes);

        int uidLen = byteBuffer.getInt();
        byte[] uidBytes = new byte[uidLen];
        byteBuffer.get(uidBytes);
        String uid = new String(uidBytes, Charset.forName("UTF-8"));

        int issuedToLen = byteBuffer.getInt();
        byte[] issuedToBytes = new byte[issuedToLen];
        byteBuffer.get(issuedToBytes);
        String issuedTo = new String(issuedToBytes, Charset.forName("UTF-8"));

        int featureLen = byteBuffer.getInt();
        byte[] featureBytes = new byte[featureLen];
        byteBuffer.get(featureBytes);
        String feature = new String(featureBytes, Charset.forName("UTF-8"));

        int maxNodes = byteBuffer.getInt();
        long issueDate = byteBuffer.getLong();
        long expiryDate = byteBuffer.getLong();

        return TrialLicensesBuilder.trialLicenseBuilder()
                .uid(uid)
                .issuedTo(issuedTo)
                .feature(feature)
                .maxNodes(maxNodes)
                .issueDate(issueDate)
                .expiryDate(expiryDate)
                .build();
    }

    public static String toEncodedTrialLicense(TrialLicenses.TrialLicense trialLicense) {
        byte[] uidBytes = trialLicense.uid().getBytes(Charset.forName("UTF-8"));
        byte[] featureBytes = trialLicense.feature().getBytes(Charset.forName("UTF-8"));
        byte[] issuedToBytes = trialLicense.issuedTo().getBytes(Charset.forName("UTF-8"));

        // uid len + uid bytes + issuedTo len + issuedTo bytes + feature bytes length + feature bytes + maxNodes + issueDate + expiryDate
        int len = 4 + uidBytes.length + 4 + issuedToBytes.length + 4 + featureBytes.length + 4 + 8 + 8;
        final byte[] encodedLicense = new byte[len];
        ByteBuffer byteBuffer = ByteBuffer.wrap(encodedLicense);

        byteBuffer.putInt(uidBytes.length);
        byteBuffer.put(uidBytes);

        byteBuffer.putInt(issuedToBytes.length);
        byteBuffer.put(issuedToBytes);

        byteBuffer.putInt(featureBytes.length);
        byteBuffer.put(featureBytes);

        byteBuffer.putInt(trialLicense.maxNodes());
        byteBuffer.putLong(trialLicense.issueDate());
        byteBuffer.putLong(trialLicense.expiryDate());

        return Base64.encodeBase64String(encodedLicense);
    }

    public static String[] toEncodedTrialLicenses(TrialLicenses trialLicenses) {
        Set<String> encodedTrialLicenses = new HashSet<>();
        for (TrialLicenses.TrialLicense trialLicense : trialLicenses) {
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
        for (TrialLicenses.TrialLicense trialLicense : trialLicenses) {
            out.writeMap(trialLicenseAsMap(trialLicense));
        }
    }

    // TODO: make sure field order is preserved
    public static Map<String, Object> trialLicenseAsMap(TrialLicenses.TrialLicense trialLicense) {
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        builder.put(TrialLicenseFields.UID, trialLicense.uid());
        builder.put(TrialLicenseFields.TYPE, ESLicenses.Type.TRIAL.string());
        builder.put(TrialLicenseFields.SUBSCRIPTION_TYPE, ESLicenses.SubscriptionType.NONE.string());
        builder.put(TrialLicenseFields.ISSUE_DATE, trialLicense.issueDate());
        builder.put(TrialLicenseFields.FEATURE, trialLicense.feature());
        builder.put(TrialLicenseFields.EXPIRY_DATE, trialLicense.expiryDate());
        builder.put(TrialLicenseFields.MAX_NODES, trialLicense.maxNodes());
        builder.put(TrialLicenseFields.ISSUED_TO, trialLicense.issuedTo());
        return builder.build();
    }

    public static TrialLicenses.TrialLicense trialLicenseFromMap(Map<String, Object> map) {
        return TrialLicensesBuilder.trialLicenseBuilder()
                .uid((String) map.get(TrialLicenseFields.UID))
                .issuedTo((String) map.get(TrialLicenseFields.ISSUED_TO))
                .maxNodes((int) map.get(TrialLicenseFields.MAX_NODES))
                .feature((String) map.get(TrialLicenseFields.FEATURE))
                .issueDate((long) map.get(TrialLicenseFields.ISSUE_DATE))
                .expiryDate((long) map.get(TrialLicenseFields.EXPIRY_DATE))
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
}
