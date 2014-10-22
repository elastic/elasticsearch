/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core.trial;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.license.core.LicensesCharset;

import java.nio.ByteBuffer;
import java.util.Set;

import static org.elasticsearch.license.plugin.core.trial.TrialLicensesBuilder.trialLicensesBuilder;

public class TrialLicenseUtils {

    public static TrialLicenses.TrialLicense fromEncodedTrialLicense(String encodedTrialLicense) {
        byte[] encodedBytes = Base64.decodeBase64(encodedTrialLicense);
        ByteBuffer byteBuffer = ByteBuffer.wrap(encodedBytes);

        int uidLen = byteBuffer.getInt();
        byte[] uidBytes = new byte[uidLen];
        byteBuffer.get(uidBytes);
        String uid = new String(uidBytes, LicensesCharset.UTF_8);

        int issuedToLen = byteBuffer.getInt();
        byte[] issuedToBytes = new byte[issuedToLen];
        byteBuffer.get(issuedToBytes);
        String issuedTo = new String(issuedToBytes, LicensesCharset.UTF_8);

        int featureLen = byteBuffer.getInt();
        byte[] featureBytes = new byte[featureLen];
        byteBuffer.get(featureBytes);
        String feature = new String(featureBytes, LicensesCharset.UTF_8);

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
        byte[] uidBytes = trialLicense.uid().getBytes(LicensesCharset.UTF_8);
        byte[] featureBytes = trialLicense.feature().getBytes(LicensesCharset.UTF_8);
        byte[] issuedToBytes = trialLicense.issuedTo().getBytes(LicensesCharset.UTF_8);

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
}
