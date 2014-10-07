/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import net.nicholaswilliams.java.licensing.*;
import net.nicholaswilliams.java.licensing.encryption.FilePublicKeyDataProvider;
import net.nicholaswilliams.java.licensing.encryption.Hasher;
import net.nicholaswilliams.java.licensing.encryption.PasswordProvider;
import net.nicholaswilliams.java.licensing.exception.ExpiredLicenseException;
import net.nicholaswilliams.java.licensing.exception.InvalidLicenseException;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.license.core.ESLicenses.*;

/**
 * Class responsible for reading signed licenses, maintaining an effective esLicenses instance, verification of licenses
 * and querying against licenses on a feature basis
 *
 * TODO:
 *  - integration with cluster state
 *  - use ESLicenseProvider to query license from cluster state
 */
public class ESLicenseManager {

    private final LicenseManager licenseManager;
    private final ESLicenses esLicenses;
    private final FilePublicKeyDataProvider publicKeyDataProvider;

    public ESLicenseManager(Set<ESLicenses> esLicensesSet, String publicKeyFile) throws IOException {
        this.publicKeyDataProvider = new FilePublicKeyDataProvider(publicKeyFile);
        this.esLicenses = merge(esLicensesSet);
        LicenseManagerProperties.setLicenseProvider(new ESLicenseProvider());
        LicenseManagerProperties.setPublicKeyDataProvider(publicKeyDataProvider);
        LicenseManagerProperties.setLicenseValidator(new DefaultLicenseValidator());
        LicenseManagerProperties.setPublicKeyPasswordProvider(new ESPublicKeyPasswordProvider());
        this.licenseManager = LicenseManager.getInstance();
    }


    public ESLicenseManager(ESLicenses esLicenses, String publicKeyFile) throws IOException {
        this(Collections.singleton(esLicenses), publicKeyFile);
    }

    private static ESLicenses merge(Set<ESLicenses> esLicensesSet) {
        ESLicenses mergedLicenses = null;
        for (ESLicenses licenses : esLicensesSet) {
            mergedLicenses = LicenseBuilders.merge(mergedLicenses, licenses);
        }
        return mergedLicenses;
    }

    public ESLicenses getEffectiveLicenses() {
        return esLicenses;
    }

    private License getLicense(FeatureType featureType) {
        ESLicense esLicense = esLicenses.get(featureType);
        if (esLicense != null) {
            String signature = esLicense.signature();
            try {
                License license = this.licenseManager.decryptAndVerifyLicense(extractSignedLicence(signature));
                this.licenseManager.validateLicense(license);
                return license;
            } catch (IOException e) {
                throw new IllegalStateException("bogus");
            }
        }
        return null;
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
     * @throws IOException
     */
    private SignedLicense extractSignedLicence(String signature) throws IOException {
        byte[] signatureBytes = Base64.decodeBase64(signature);
        ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
        byteBuffer = (ByteBuffer) byteBuffer.position(13);
        int start = byteBuffer.getInt();
        int version = byteBuffer.getInt();
        byte[] hash = new byte[start - 13 - 4 - 4];
        byteBuffer.get(hash);

        final byte[] computedHash = Hasher.hash(Base64.encodeBase64String(
                        Files.readAllBytes(Paths.get(publicKeyDataProvider.getPublicKeyFile().getAbsolutePath())))
        ).getBytes(Charset.forName("UTF-8"));

        if (!Arrays.equals(hash, computedHash)) {
            throw new InvalidLicenseException("Invalid License");
        }

        return new ObjectSerializer().readObject(SignedLicense.class, Arrays.copyOfRange(signatureBytes, start, signatureBytes.length));
    }


    public void verifyLicenses() {
        for (FeatureType featureType : esLicenses.features()) {
            final License license = getLicense(featureType);
            assert license != null : "license should not be null for feature: " + featureType.string();
            verifyLicenseFields(license, esLicenses.get(featureType));
        }
    }


    private static void verifyLicenseFields(License license, ESLicense eslicense) {
        boolean licenseValid = license.getProductKey().equals(eslicense.uid())
                && license.getHolder().equals(eslicense.issuedTo())
                && license.getIssueDate() == eslicense.issueDate()
                && license.getGoodBeforeDate() == eslicense.expiryDate();
        assert license.getFeatures().size() == 4 : "one license should have only four features";
        String maxNodesPrefix = "maxNodes:";
        String typePrefix = "type:";
        String subscriptionTypePrefix = "subscription_type:";
        boolean maxNodesValid = false;
        boolean featureValid = false;
        boolean typeValid = false;
        boolean subscriptionTypeValid = false;

        for (License.Feature feature : license.getFeatures()) {
            String featureName = feature.getName();
            if (featureName.startsWith(maxNodesPrefix)) {
                maxNodesValid = eslicense.maxNodes() == Integer.parseInt(featureName.substring(maxNodesPrefix.length()));
            } else if (featureName.startsWith(typePrefix)) {
                typeValid = eslicense.type() == Type.fromString(featureName.substring(typePrefix.length()));
            } else if (featureName.startsWith(subscriptionTypePrefix)) {
                subscriptionTypeValid = eslicense.subscriptionType() == SubscriptionType.fromString(featureName.substring(subscriptionTypePrefix.length()));
            } else {
                featureValid = feature.getName().equals(eslicense.feature().string())
                        && feature.getGoodBeforeDate() == eslicense.expiryDate();
            }
        }
        if (!licenseValid || !featureValid || !maxNodesValid || !typeValid || !subscriptionTypeValid) {
            //only for debugging
            String msg = "licenseValid: " + licenseValid + "\n" +
                    "featureValid: " + featureValid + "\n" +
                    "maxNodeValide: " + maxNodesValid + "\n" +
                    "typeValid: " + typeValid + "\n" +
                    "subscriptionTypeValid: " + subscriptionTypeValid + "\n";
            throw new InvalidLicenseException("Invalid License");
        }
    }


    public boolean hasLicenseForFeature(FeatureType featureType) {
        try {
            final License license = getLicense(featureType);
            if (license == null) {
                return false;
            }
            return license.hasLicenseForFeature(featureType.string());
        } catch (ExpiredLicenseException e) {
            return false;
        } catch (InvalidLicenseException e) {
            return false;
        }
    }

    public boolean hasLicenseForNodes(FeatureType featureType, int nodes) {
        ESLicense esLicense = generateESLicense(featureType);
        return esLicense.maxNodes() >= nodes;
    }

    public String getIssuerForLicense(FeatureType featureType) {
        final License license = getLicense(featureType);
        return license.getIssuer();
    }

    public long getIssueDateForLicense(FeatureType featureType) {
        final License license = getLicense(featureType);
        return license.getIssueDate();
    }

    public long getExpiryDateForLicense(FeatureType featureType) {
        final License license = getLicense(featureType);
        return license.getGoodBeforeDate();
    }

    public String getIssuedToForLicense(FeatureType featureType) {
        final License license = getLicense(featureType);
        return license.getHolder();
    }

    public Type getTypeForLicense(FeatureType featureType) {
        ESLicense esLicense = generateESLicense(featureType);
        return esLicense.type();
    }

    public SubscriptionType getSubscriptionTypeForLicense(FeatureType featureType) {
        ESLicense esLicense = generateESLicense(featureType);
        return esLicense.subscriptionType();
    }

    private ESLicense generateESLicense(FeatureType featureType) {
        final License license = getLicense(featureType);
        return convertToESLicense(license);
    }

    static ESLicense convertToESLicense(License license) {
        final LicenseBuilders.LicenseBuilder licenseBuilder = LicenseBuilders.licenseBuilder(false);
        licenseBuilder
                .expiryDate(license.getGoodBeforeDate())
                .issueDate(license.getIssueDate())
                .uid(license.getProductKey())
                .issuedTo(license.getHolder())
                .issuer(license.getIssuer());

        assert license.getFeatures().size() == 4 : "one license should have only four feature";
        String maxNodesPrefix = "maxNodes:";
        String typePrefix = "type:";
        String subscriptionTypePrefix = "subscription_type:";
        for (License.Feature feature : license.getFeatures()) {
            String featureName = feature.getName();
            if (featureName.startsWith(maxNodesPrefix)) {
                licenseBuilder.maxNodes(Integer.parseInt(featureName.substring(maxNodesPrefix.length())));
            } else if (featureName.startsWith(typePrefix)) {
                licenseBuilder.type(Type.fromString(featureName.substring(typePrefix.length())));
            } else if (featureName.startsWith(subscriptionTypePrefix)) {
                licenseBuilder.subscriptionType(SubscriptionType.fromString(featureName.substring(subscriptionTypePrefix.length())));
            } else {
                licenseBuilder.feature(FeatureType.fromString(featureName));
            }
        }
        return licenseBuilder.build();
    }

    /**
     * Used by the underlying license manager (make sure it is never called for now)
     * This should be retrieving licenses from the custom metadata in the cluster state
     */
    public class ESLicenseProvider implements LicenseProvider {
        @Override
        public SignedLicense getLicense(Object context) {
            throw new NotImplementedException();
        }
    }

    private class ESPublicKeyPasswordProvider implements PasswordProvider {
        private final String DEFAULT_PASS_PHRASE = "elasticsearch-license";

        @Override
        public char[] getPassword() {
            return Hasher.hash(DEFAULT_PASS_PHRASE).toCharArray();
        }
    }
}
