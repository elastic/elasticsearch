/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import net.nicholaswilliams.java.licensing.LicenseManager;
import net.nicholaswilliams.java.licensing.ObjectSerializer;
import net.nicholaswilliams.java.licensing.SignedLicense;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

import static org.elasticsearch.license.core.ESLicenses.ESLicense;

public class Utils {

    private Utils() {
    }

    static ESLicenses getESLicensesFromSignatures(final LicenseManager licenseManager, Set<String> signatures) {
        final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
        for (String signature : signatures) {
            licensesBuilder.license(getESLicenseFromSignature(licenseManager, signature));
        }
        return licensesBuilder.build();
    }

    private static ESLicense getESLicenseFromSignature(LicenseManager licenseManager, String signature) {
        byte[] signatureBytes = Base64.decodeBase64(signature);
        ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
        byteBuffer = (ByteBuffer) byteBuffer.position(13);
        int start = byteBuffer.getInt();
        SignedLicense signedLicense = new ObjectSerializer()
                .readObject(SignedLicense.class, Arrays.copyOfRange(signatureBytes, start, signatureBytes.length));
        return ESLicenseManager.convertToESLicense(licenseManager.decryptAndVerifyLicense(signedLicense));
    }


}
