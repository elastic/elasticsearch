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
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.ESLicense;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Set;

public class ESLicenseSigner {

    public static String DEFAULT_PASS_PHRASE = "elasticsearch-license";

    private final static int VERSION_START = 0;
    private final static int VERSION = VERSION_START;

    private final static int MAGIC_LENGTH = 13;

    private final LicenseCreator licenseCreator;

    private final Path publicKeyPath;

    public ESLicenseSigner(final String privateKeyPath, final String publicKeyPath) {
        this(Paths.get(privateKeyPath), Paths.get(publicKeyPath));
    }

    public ESLicenseSigner(final Path privateKeyPath, final Path publicKeyPath) {
        LicenseCreatorProperties.setPrivateKeyDataProvider(new PrivateKeyDataProvider() {
            @Override
            public byte[] getEncryptedPrivateKeyData() throws KeyNotFoundException {
                assert privateKeyPath.toFile().exists();
                try {
                    return Files.readAllBytes(privateKeyPath);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new IllegalStateException(e);
                }

            }
        });
        LicenseCreatorProperties.setPrivateKeyPasswordProvider(new PasswordProvider() {

            @Override
            public char[] getPassword() {
                return Hasher.hash(DEFAULT_PASS_PHRASE).toCharArray();
            }
        });
        this.licenseCreator = LicenseCreator.getInstance();
        this.publicKeyPath = publicKeyPath;
    }


    public ImmutableSet<ESLicense> sign(Set<ESLicense> licenseSpecs) throws IOException {
        final ImmutableSet.Builder<ESLicense> builder = ImmutableSet.builder();
        for (ESLicense licenseSpec : licenseSpecs) {
            builder.add(sign(licenseSpec));
        }
        return builder.build();
    }

    /**
     * Generates a signature for the <code>esLicense</code>.
     * Signature structure:
     * | MAGIC | HEADER_LENGTH | VERSION | PUB_KEY_DIGEST | SIGNED_LICENSE_CONTENT |
     *
     * @return a signed ESLicense (with signature)
     * @throws IOException
     */
    public ESLicense sign(ESLicense licenseSpec) throws IOException {
        License.Builder licenseBuilder = new License.Builder()
                .withGoodBeforeDate(licenseSpec.expiryDate())
                .withIssueDate(licenseSpec.issueDate())
                .withProductKey(licenseSpec.uid())
                .withHolder(licenseSpec.issuedTo())
                .withIssuer(licenseSpec.issuer());

        // NOTE: to add additional feature(s) to the internal license
        // encode the new feature(s) in featureToXContent rather
        // than doing licenseBuilder.addFeature(..)
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        featureToXContent(licenseSpec, contentBuilder);
        licenseBuilder.addFeature(contentBuilder.string());

        final License license = licenseBuilder.build();

        final byte[] magic = new byte[MAGIC_LENGTH];
        Random random = new Random();
        random.nextBytes(magic);
        final byte[] licenseSignature = licenseCreator.signAndSerializeLicense(license);
        final byte[] hash = Hasher.hash(Base64.encodeBase64String(
                        Files.readAllBytes(publicKeyPath))
        ).getBytes(StandardCharsets.UTF_8);
        int headerLength = MAGIC_LENGTH + hash.length + 4 + 4;
        byte[] bytes = new byte[headerLength + licenseSignature.length];

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.put(magic)
                .putInt(headerLength)
                .putInt(VERSION)
                .put(hash)
                .put(licenseSignature);
        String signature = Base64.encodeBase64String(bytes);

        return ESLicense.builder()
                .fromLicenseSpec(licenseSpec, signature)
                .verify()
                .build();
    }

    private void featureToXContent(ESLicense license, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("feature", license.feature());
        builder.field("type", license.type());
        builder.field("subscription_type", license.subscriptionType());
        builder.field("max_nodes", license.maxNodes());
        builder.endObject();
    }

}
