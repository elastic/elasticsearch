/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import net.nicholaswilliams.java.licensing.License;
import net.nicholaswilliams.java.licensing.encryption.Hasher;
import net.nicholaswilliams.java.licensing.encryption.PasswordProvider;
import net.nicholaswilliams.java.licensing.encryption.PrivateKeyDataProvider;
import net.nicholaswilliams.java.licensing.exception.KeyNotFoundException;
import net.nicholaswilliams.java.licensing.licensor.LicenseCreator;
import net.nicholaswilliams.java.licensing.licensor.LicenseCreatorProperties;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;

import static org.elasticsearch.license.core.ESLicenses.ESLicense;

public class ESLicenseSigner {

    private final static int VERSION_START = 0;
    private final static int VERSION = VERSION_START;

    private final static int MAGIC_LENGTH = 13;

    private final LicenseCreator licenseCreator;
    private final SignerOptions options;

    public static class SignerOptions {
        final String privateKeyPath;
        final String publicKeyPath;
        final String password;

        public SignerOptions(String privateKeyPath, String publicKeyPath, String password) {
            this.privateKeyPath = privateKeyPath;
            this.publicKeyPath = publicKeyPath;
            this.password = password;
        }
    }

    public ESLicenseSigner(final SignerOptions options) {
        LicenseCreatorProperties.setPrivateKeyDataProvider(new PrivateKeyDataProvider() {
            @Override
            public byte[] getEncryptedPrivateKeyData() throws KeyNotFoundException {
                Path privateKeyFile = Paths.get(options.privateKeyPath);
                assert privateKeyFile.toFile().exists();
                try {
                    return Files.readAllBytes(privateKeyFile);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IllegalStateException(e);
                }

            }
        });
        LicenseCreatorProperties.setPrivateKeyPasswordProvider(new PasswordProvider() {
            @Override
            public char[] getPassword() {
                return options.password.toCharArray();
            }
        });
        this.licenseCreator = LicenseCreator.getInstance();
        this.options = options;
    }

    public ESLicenses sign(ESLicenses esLicenses) throws IOException {
        final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
        for (ESLicense license : esLicenses) {
            licensesBuilder.license(sign(license));
        }
        return licensesBuilder.build();
    }

    /**
     * Generates a signature for the <code>esLicense</code>.
     * Signature structure:
     * | MAGIC | HEADER_LENGTH | VERSION | PUB_KEY_DIGEST | SIGNED_LICENSE_CONTENT |
     *
     * @return a signed ESLicense (with signature)
     * @throws IOException
     */
    public ESLicense sign(ESLicense esLicense) throws IOException {
        License.Builder licenseBuilder = new License.Builder()
                .withGoodBeforeDate(esLicense.expiryDate())
                .withIssueDate(esLicense.issueDate())
                .withProductKey(esLicense.uid())
                .withHolder(esLicense.issuedTo())
                .withIssuer(esLicense.issuer())
                .addFeature(esLicense.feature().string(), esLicense.expiryDate())
                .addFeature("maxNodes:" + String.valueOf(esLicense.maxNodes()))
                .addFeature("type:" + esLicense.type().string())
                .addFeature("subscription_type:" + esLicense.subscriptionType().string());

        final License license = licenseBuilder.build();

        final byte[] magic = new byte[MAGIC_LENGTH];
        Random random = new Random();
        random.nextBytes(magic);
        final byte[] licenseSignature = licenseCreator.signAndSerializeLicense(license);
        final byte[] hash = Hasher.hash(Base64.encodeBase64String(
                        Files.readAllBytes(Paths.get(options.publicKeyPath)))
        ).getBytes(Charset.forName("UTF-8"));
        int headerLength = MAGIC_LENGTH + hash.length + 4 + 4;
        byte[] bytes = new byte[headerLength + licenseSignature.length];

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.put(magic)
                .putInt(headerLength)
                .putInt(VERSION)
                .put(hash)
                .put(licenseSignature);
        String signature = Base64.encodeBase64String(bytes);

        return LicenseBuilders.licenseBuilder(true).fromLicense(esLicense).signature(signature).build();
    }
}
