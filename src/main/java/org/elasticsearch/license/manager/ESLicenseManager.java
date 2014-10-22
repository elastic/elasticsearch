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
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.license.core.ESLicense;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.license.core.ESLicense.SubscriptionType;
import static org.elasticsearch.license.core.ESLicense.Type;
import static org.elasticsearch.license.manager.Utils.extractSignedLicence;

/**
 * Class responsible for reading signed licenses, maintaining an effective esLicenses instance, verification of licenses
 * and querying against licenses on a feature basis
 * <p/>
 * TODO:
 *  - make it into a guice singleton
 */
public class ESLicenseManager {

    private final LicenseManager licenseManager;

    private static class Prefix {
        static final String MAX_NODES = "maxNodes:";
        static final String TYPE = "type:";
        static final String SUBSCRIPTION_TYPE = "subscription_type:";
        static final String FEATURE = "feature:";
    }

    // Initialize LicenseManager
    static {
        LicenseManagerProperties.setPublicKeyDataProvider(new FilePublicKeyDataProvider(getPublicKeyPath()));
        LicenseManagerProperties.setPublicKeyPasswordProvider(new ESPublicKeyPasswordProvider());
        LicenseManagerProperties.setLicenseValidator(new DefaultLicenseValidator());
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

    public ESLicenseManager() {
        this.licenseManager = LicenseManager.getInstance();
    }

    public void verifyLicenses(Map<String, org.elasticsearch.license.core.ESLicense> esLicenses) {
        try {
            for (String feature : esLicenses.keySet()) {
                org.elasticsearch.license.core.ESLicense esLicense = esLicenses.get(feature);
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
            throw new InvalidLicenseException("Invalid License");
        }
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
            ESLicense license = fromSignature(signature);
            esLicenses.add(license);
        }
        return ImmutableSet.copyOf(esLicenses);
    }

    public ESLicense fromSignature(String signature) {
        final SignedLicense signedLicense = Utils.extractSignedLicence(signature);
        License license = licenseManager.decryptAndVerifyLicense(signedLicense);
        ESLicense.Builder builder = ESLicense.builder();



        for (License.Feature feature : license.getFeatures()) {
            String featureName = feature.getName();
            if (featureName.startsWith(Prefix.MAX_NODES)) {
                builder.maxNodes(Integer.parseInt(featureName.substring(Prefix.MAX_NODES.length())));
            } else if (featureName.startsWith(Prefix.TYPE)) {
                builder.type(Type.fromString(featureName.substring(Prefix.TYPE.length())));
            } else if (featureName.startsWith(Prefix.SUBSCRIPTION_TYPE)) {
                builder.subscriptionType(SubscriptionType.fromString(featureName.substring(Prefix.SUBSCRIPTION_TYPE.length())));
            } else if (featureName.startsWith(Prefix.FEATURE)) {
                builder.feature(featureName.substring(Prefix.FEATURE.length()));
            }
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

    private static void verifyLicenseFields(License license, ESLicense eslicense) {
        boolean licenseValid = license.getProductKey().equals(eslicense.uid())
                && license.getHolder().equals(eslicense.issuedTo())
                && license.getIssueDate() == eslicense.issueDate()
                && license.getGoodBeforeDate() == eslicense.expiryDate();
        assert license.getFeatures().size() == 4 : "one license should have only four features";
        boolean maxNodesValid = false;
        boolean featureValid = false;
        boolean typeValid = false;
        boolean subscriptionTypeValid = false;

        for (License.Feature feature : license.getFeatures()) {
            String featureName = feature.getName();
            if (featureName.startsWith(Prefix.MAX_NODES)) {
                maxNodesValid = eslicense.maxNodes() == Integer.parseInt(featureName.substring(Prefix.MAX_NODES.length()));
            } else if (featureName.startsWith(Prefix.TYPE)) {
                typeValid = eslicense.type() == Type.fromString(featureName.substring(Prefix.TYPE.length()));
            } else if (featureName.startsWith(Prefix.SUBSCRIPTION_TYPE)) {
                subscriptionTypeValid = eslicense.subscriptionType() == SubscriptionType.fromString(featureName.substring(Prefix.SUBSCRIPTION_TYPE.length()));
            } else if (featureName.startsWith(Prefix.FEATURE)) {
                String featureValue = featureName.substring(Prefix.FEATURE.length());
                featureValid = featureValue.equals(eslicense.feature())
                        && feature.getGoodBeforeDate() == eslicense.expiryDate();
            }
        }
        if (!licenseValid || !featureValid || !maxNodesValid || !typeValid || !subscriptionTypeValid) {
            //only for debugging
            String msg = "licenseValid: " + licenseValid + "\n" +
                    "featureValid: " + featureValid + "\n" +
                    "maxNodeValid: " + maxNodesValid + "\n" +
                    "typeValid: " + typeValid + "\n" +
                    "subscriptionTypeValid: " + subscriptionTypeValid + "\n";
            throw new InvalidLicenseException("Invalid License");
        }
    }


    public boolean hasLicenseForFeature(String feature, Map<String, ESLicense> licenseMap) {
        try {
            final License license = getInternalLicense(feature, licenseMap);
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

    private License getInternalLicense(String feature,  Map<String, ESLicense> licenseMap) {
        ESLicense esLicense = licenseMap.get(feature);
        if (esLicense != null) {
            String signature = esLicense.signature();
            License license = this.licenseManager.decryptAndVerifyLicense(extractSignedLicence(signature));
            this.licenseManager.validateLicense(license);
            return license;
        }
        return null;
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
