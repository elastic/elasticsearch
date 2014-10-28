/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import net.nicholaswilliams.java.licensing.*;
import net.nicholaswilliams.java.licensing.encryption.Hasher;
import net.nicholaswilliams.java.licensing.encryption.PasswordProvider;
import net.nicholaswilliams.java.licensing.exception.ExpiredLicenseException;
import net.nicholaswilliams.java.licensing.exception.InvalidLicenseException;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ResourcePublicKeyDataProvider;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Class responsible for reading signed licenses, maintaining an effective esLicenses instance, verification of licenses
 * and querying against licenses on a feature basis
 * <p/>
 */
public class ESLicenseManager {

    private final LicenseManager licenseManager;

    private static class FeatureFields {
        static final String MAX_NODES = "max_nodes";
        static final String TYPE = "type";
        static final String SUBSCRIPTION_TYPE = "subscription_type";
        static final String FEATURE = "feature";
    }

    // Initialize LicenseManager
    static {
        LicenseManagerProperties.setPublicKeyDataProvider(new ResourcePublicKeyDataProvider("/public.key"));
        LicenseManagerProperties.setPublicKeyPasswordProvider(new ESPublicKeyPasswordProvider());
        LicenseManagerProperties.setLicenseValidator(new DefaultLicenseValidator());
        LicenseManagerProperties.setLicenseProvider(new LicenseProvider() {
            @Override
            public SignedLicense getLicense(Object context) {
                throw new UnsupportedOperationException("This singleton license provider shouldn't be used");
            }
        });
    }

    @Inject
    public ESLicenseManager() {
        this.licenseManager = LicenseManager.getInstance();
    }

    public ImmutableSet<String> toSignatures(Collection<ESLicense> esLicenses) {
        Set<String> signatures = new HashSet<>();
        for (ESLicense esLicense : esLicenses) {
            signatures.add(esLicense.signature());
        }
        return ImmutableSet.copyOf(signatures);
    }

    public ImmutableSet<ESLicense> fromSignatures(Set<String> signatures) {
        Set<ESLicense> esLicenses = new HashSet<>();
        for (String signature : signatures) {
            esLicenses.add(fromSignature(signature));
        }
        return ImmutableSet.copyOf(esLicenses);
    }

    public void verifyLicenses(Map<String, ESLicense> esLicenses) {
        try {
            for (String feature : esLicenses.keySet()) {
                ESLicense esLicense = esLicenses.get(feature);
                // verify signature
                final License license = this.licenseManager.decryptAndVerifyLicense(
                        extractSignedLicence(esLicense.signature()));
                // validate license
                this.licenseManager.validateLicense(license);

                // verify all readable license fields
                verifyLicenseFields(license, esLicense);
            }
        } catch (ExpiredLicenseException e) {
            throw new InvalidLicenseException("Expired License");
        } catch (InvalidLicenseException | IOException e) {
            throw new InvalidLicenseException("Invalid License");
        }
    }

    private ESLicense fromSignature(String signature) {
        final SignedLicense signedLicense = extractSignedLicence(signature);
        License license = licenseManager.decryptAndVerifyLicense(signedLicense);
        ESLicense.Builder builder = ESLicense.builder();

        for (License.Feature feature : license.getFeatures()) {
            String featureName = feature.getName();
            LicenseFeatures licenseFeatures;
            try {
                licenseFeatures = licenseFeaturesFromSource(featureName);
                if (licenseFeatures.maxNodes != -1
                        && licenseFeatures.feature != null
                        && licenseFeatures.type != null
                        && licenseFeatures.subscriptionType != null) {
                    builder.maxNodes(licenseFeatures.maxNodes)
                            .feature(licenseFeatures.feature)
                            .type(licenseFeatures.type)
                            .subscriptionType(licenseFeatures.subscriptionType);
                    break;
                }
            } catch (IOException ignored) {}
        }

        return builder
                .uid(license.getProductKey())
                .issuer(license.getIssuer())
                .issuedTo(license.getHolder())
                .issueDate(license.getIssueDate())
                .expiryDate(license.getGoodBeforeDate())
                .signature(signature)
                .build();
    }

    private static void verifyLicenseFields(License license, ESLicense eslicense) throws IOException {
        boolean licenseValid = license.getProductKey().equals(eslicense.uid())
                && license.getHolder().equals(eslicense.issuedTo())
                && license.getIssueDate() == eslicense.issueDate()
                && license.getGoodBeforeDate() == eslicense.expiryDate();
        boolean maxNodesValid = false;
        boolean featureValid = false;
        boolean typeValid = false;
        boolean subscriptionTypeValid = false;

        for (License.Feature feature : license.getFeatures()) {
            String featureName = feature.getName();
            LicenseFeatures licenseFeatures = licenseFeaturesFromSource(featureName);
            maxNodesValid = eslicense.maxNodes() == licenseFeatures.maxNodes;
            typeValid = eslicense.type().equals(licenseFeatures.type);
            subscriptionTypeValid = eslicense.subscriptionType().equals(licenseFeatures.subscriptionType);
            featureValid = eslicense.feature().equals(licenseFeatures.feature);

            if (maxNodesValid && typeValid && subscriptionTypeValid && featureValid) {
                break;
            }
        }
        if (!licenseValid || !featureValid || !maxNodesValid || !typeValid || !subscriptionTypeValid) {
            throw new InvalidLicenseException("Invalid License");
        }
    }

    /**
     * Extract a signedLicense (SIGNED_LICENSE_CONTENT) from the signature.
     * Validates the public key used to decrypt the license by comparing their hashes
     * <p/>
     * Signature structure:
     * | MAGIC | HEADER_LENGTH | VERSION | PUB_KEY_DIGEST | SIGNED_LICENSE_CONTENT |
     *
     * @param signature of a single license
     * @return signed license content for the license
     */
    private static SignedLicense extractSignedLicence(String signature) {
        byte[] signatureBytes = Base64.decodeBase64(signature);
        ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
        byteBuffer = (ByteBuffer) byteBuffer.position(13);
        int start = byteBuffer.getInt();
        int version = byteBuffer.getInt();
        return new ObjectSerializer().readObject(SignedLicense.class, Arrays.copyOfRange(signatureBytes, start, signatureBytes.length));
    }

    private static LicenseFeatures licenseFeaturesFromSource(String source) throws IOException {
        XContentParser parser = XContentFactory.xContent(source).createParser(source);
        XContentParser.Token token;

        String feature = null;
        String type = null;
        String subscriptionType = null;
        int maxNodes = -1;

        String currentName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                switch (currentName) {
                    case FeatureFields.FEATURE:
                        feature = parser.text();
                        break;
                    case FeatureFields.TYPE:
                        type = parser.text();
                        break;
                    case FeatureFields.SUBSCRIPTION_TYPE:
                        subscriptionType = parser.text();
                        break;
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (FeatureFields.MAX_NODES.equals(currentName)) {
                    maxNodes = parser.intValue();
                }
            }
        }
        return new LicenseFeatures(feature, type, subscriptionType, maxNodes);
    }

    private static class LicenseFeatures {
        private final String feature;
        private final String type;
        private final String subscriptionType;
        private final int maxNodes;

        private LicenseFeatures(String feature, String type, String subscriptionType, int maxNodes) {
            this.feature = feature;
            this.type = type;
            this.subscriptionType = subscriptionType;
            this.maxNodes = maxNodes;
        }
    }

    // TODO: Need a better password management
    private static class ESPublicKeyPasswordProvider implements PasswordProvider {
        private final String DEFAULT_PASS_PHRASE = "elasticsearch-license";

        @Override
        public char[] getPassword() {
            return Hasher.hash(DEFAULT_PASS_PHRASE).toCharArray();
        }
    }
}
