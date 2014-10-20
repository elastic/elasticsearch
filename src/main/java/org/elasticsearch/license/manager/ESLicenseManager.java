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
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Set;

import static org.elasticsearch.license.core.ESLicenses.*;
import static org.elasticsearch.license.manager.Utils.extractSignedLicence;

/**
 * Class responsible for reading signed licenses, maintaining an effective esLicenses instance, verification of licenses
 * and querying against licenses on a feature basis
 * <p/>
 * TODO:
 *  - make it into a guice singleton
 */
public class ESLicenseManager {

    private final ESLicenseProvider licenseProvider;

    private final LicenseManager licenseManager;

    // Initialize LicenseManager
    static {
        LicenseManagerProperties.setPublicKeyDataProvider(new FilePublicKeyDataProvider(getPublicKeyPath()));
        LicenseManagerProperties.setPublicKeyPasswordProvider(new ESPublicKeyPasswordProvider());
        LicenseManagerProperties.setLicenseProvider(new LicenseProvider() {
            @Override
            public SignedLicense getLicense(Object context) {
                throw new UnsupportedOperationException("This singelton license provider shouldn't be used");
            }
        });
    }

    private static String getPublicKeyPath() {
        //TODO: Imporove key management
        URL resource = ESLicenseManager.class.getResource("public.key");
        if (resource == null) {
            //test REMOVE NOCOMMIT!!!!
            resource = ESLicenseManager.class.getResource("/org.elasticsearch.license.plugin/test_pub.key");
        }
        try {
            return Paths.get(resource.toURI()).toFile().getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }
    }

    public ESLicenseManager(ESLicenseProvider licenseProvider) {
        this.licenseProvider = licenseProvider;
        this.licenseManager = LicenseManager.getInstance();
    }

    private static ESLicenses merge(Set<ESLicenses> esLicensesSet) {
        ESLicenses mergedLicenses = null;
        for (ESLicenses licenses : esLicensesSet) {
            mergedLicenses = LicenseBuilders.merge(mergedLicenses, licenses);
        }
        return mergedLicenses;
    }

    public ESLicenses getEffectiveLicenses() {
        return licenseProvider.getEffectiveLicenses();
    }

    public void verifyLicenses(ESLicenses esLicenses) {
        try {
            for (String feature : esLicenses.features()) {
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
        } catch (InvalidLicenseException e) {
            throw new InvalidLicenseException("Invalid License: " + e.getCause());
        }
    }

    public void verifyLicenses() {
        verifyLicenses(getEffectiveLicenses());
    }

    public ESLicenses fromSignaturesAsIs(final Set<String> signatures) {
        final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
        for (String signature : signatures) {
            licensesBuilder.licenseAsIs(getESLicenseFromSignature(signature));
        }
        return licensesBuilder.build();
    }

    public ESLicense getESLicenseFromSignature(String signature) {
        SignedLicense signedLicense = extractSignedLicence(signature);
        return decryptAndVerifyESLicense(signedLicense, signature);
    }

    public ESLicenses fromSignatures(final Set<String> signatures) {
        final LicenseBuilders.LicensesBuilder licensesBuilder = LicenseBuilders.licensesBuilder();
        for (String signature : signatures) {
            licensesBuilder.license(getESLicenseFromSignature(signature));
        }
        return licensesBuilder.build();
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
        String featurePrefix = "feature:";
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
            } else if (featureName.startsWith(featurePrefix)) {
                String featureValue = featureName.substring(featurePrefix.length());
                featureValid = featureValue.equals(eslicense.feature())
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
    private License getLicense(String feature) {
        ESLicense esLicense = licenseProvider.getESLicense(feature);
        if (esLicense != null) {
            String signature = esLicense.signature();
            License license = this.licenseManager.decryptAndVerifyLicense(extractSignedLicence(signature));
            this.licenseManager.validateLicense(license);
            return license;
        }
        return null;
    }

    //TODO wrap License validation methods so a plugin does not have to provide featureType param

    public boolean hasLicenseForFeature(String feature) {
        try {
            final License license = getLicense(feature);
            if (license != null) {
                return license.hasLicenseForFeature("feature:" + feature);
            }
            return false;
        } catch (ExpiredLicenseException e) {
            return false;
        } catch (InvalidLicenseException e) {
            return false;
        }
    }

    public boolean hasLicenseForNodes(String featureType, int nodes) {
        ESLicense esLicense = getESLicense(featureType);
        return esLicense.maxNodes() >= nodes;
    }

    public String getIssuerForLicense(String featureType) {
        final License license = getLicense(featureType);
        return license.getIssuer();
    }

    public long getIssueDateForLicense(String featureType) {
        final License license = getLicense(featureType);
        return license.getIssueDate();
    }

    public long getExpiryDateForLicense(String featureType) {
        final License license = getLicense(featureType);
        return license.getGoodBeforeDate();
    }

    public String getIssuedToForLicense(String featureType) {
        final License license = getLicense(featureType);
        return license.getHolder();
    }

    public Type getTypeForLicense(String featureType) {
        ESLicense esLicense = getESLicense(featureType);
        return esLicense.type();
    }

    public SubscriptionType getSubscriptionTypeForLicense(String featureType) {
        ESLicense esLicense = getESLicense(featureType);
        return esLicense.subscriptionType();
    }

    ESLicense getESLicense(String featureType) {
        final License license = getLicense(featureType);
        return convertToESLicense(license);
    }

    ESLicense decryptAndVerifyESLicense(SignedLicense signedLicense, String signature) {
        return convertToESLicense(this.licenseManager.decryptAndVerifyLicense(signedLicense), signature);
    }

    static ESLicense convertToESLicense(License license) {
        return convertToESLicense(license, null);
    }

    static ESLicense convertToESLicense(License license, String signature) {
        final LicenseBuilders.LicenseBuilder licenseBuilder = LicenseBuilders.licenseBuilder(false);
        licenseBuilder
                .expiryDate(license.getGoodBeforeDate())
                .issueDate(license.getIssueDate())
                .uid(license.getProductKey())
                .issuedTo(license.getHolder())
                .issuer(license.getIssuer());

        if (signature != null) {
            licenseBuilder.signature(signature);
        }

        assert license.getFeatures().size() == 4 : "one license should have only four feature";
        String maxNodesPrefix = "maxNodes:";
        String typePrefix = "type:";
        String subscriptionTypePrefix = "subscription_type:";
        String featurePrefix = "feature:";
        for (License.Feature feature : license.getFeatures()) {
            String featureName = feature.getName();
            if (featureName.startsWith(maxNodesPrefix)) {
                licenseBuilder.maxNodes(Integer.parseInt(featureName.substring(maxNodesPrefix.length())));
            } else if (featureName.startsWith(typePrefix)) {
                licenseBuilder.type(Type.fromString(featureName.substring(typePrefix.length())));
            } else if (featureName.startsWith(subscriptionTypePrefix)) {
                licenseBuilder.subscriptionType(SubscriptionType.fromString(featureName.substring(subscriptionTypePrefix.length())));
            } else if (featureName.startsWith(featurePrefix)) {
                licenseBuilder.feature(featureName.substring(featurePrefix.length()));
            }
        }
        return licenseBuilder.build();
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
