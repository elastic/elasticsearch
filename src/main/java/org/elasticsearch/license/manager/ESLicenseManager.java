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
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ResourcePublicKeyDataProvider;

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
        } catch (InvalidLicenseException e) {
            throw new InvalidLicenseException("Invalid License");
        }
    }

    private ESLicense fromSignature(String signature) {
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
            throw new InvalidLicenseException("Invalid License");
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
