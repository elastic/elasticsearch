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
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.license.core.ESLicenses.*;
import static org.elasticsearch.license.manager.ESLicenseProvider.ClusterStateLicenseProvider;
import static org.elasticsearch.license.manager.ESLicenseProvider.FileBasedESLicenseProvider;
import static org.elasticsearch.license.manager.ESLicenseProvider.extractSignedLicence;

/**
 * Class responsible for reading signed licenses, maintaining an effective esLicenses instance, verification of licenses
 * and querying against licenses on a feature basis
 * <p/>
 * TODO:
 * - integration with cluster state
 * - use ESLicenseProvider to query license from cluster state
 */
public class ESLicenseManager {

    private static ESLicenseManager instance = null;
    private static FilePublicKeyDataProvider publicKeyDataProvider;
    private final ESLicenseProvider licenseProvider;

    private final LicenseManager licenseManager;

    public static ESLicenseManager getInstance() {
        if (ESLicenseManager.instance == null) {
            throw new IllegalStateException("License manager has not been created!");
        }
        return ESLicenseManager.instance;
    }

    /**
     * Creates a LicenseManager instance where the Licenses are queried from the cluster state
     *
     * @param clusterService used to query for appropriate license(s) for validation
     * @param publicKeyPath used to decrypt the licenses
     * @return {@link org.elasticsearch.license.manager.ESLicenseManager} instance backed by licenses residing
     * in the cluster state
     */
    public static ESLicenseManager createClusterStateBasedInstance(ClusterService clusterService, String publicKeyPath) {
        if (ESLicenseManager.instance == null) {
            ESLicenseManager.publicKeyDataProvider = new FilePublicKeyDataProvider(publicKeyPath);
            return new ESLicenseManager(ESLicenseProvider.createClusterBasedLicenseProvider(clusterService, publicKeyPath));
        } else if (ESLicenseManager.instance.licenseProvider instanceof ClusterStateLicenseProvider) {
            return ESLicenseManager.instance;
        } else {
            throw new IllegalStateException("Manager already initiated with File based license provider");
        }
    }

    public static ESLicenseManager createClusterStateBasedInstance(ClusterService clusterService) {
        return createClusterStateBasedInstance(clusterService, getPublicKeyPath());
    }

    public static ESLicenseManager createLocalBasedInstance(ESLicenses esLicenses, String publicKeyPath) {
        return createLocalBasedInstance(Collections.singleton(esLicenses), publicKeyPath);
    }

    /**
     * Creates a LicenseManager instance where the Licenses are queried from a set of pre-generated licenses
     * @param esLicensesSet a set of pre-generated licenses stored in the license manager
     * @param publicKeyPath used to decrypt the licenses
     * @return {@link org.elasticsearch.license.manager.ESLicenseManager} instance backed by pre-generated licenses
     */
    public static ESLicenseManager createLocalBasedInstance(Set<ESLicenses> esLicensesSet, String publicKeyPath) {
        if (ESLicenseManager.instance == null) {
            ESLicenseManager.publicKeyDataProvider = new FilePublicKeyDataProvider(publicKeyPath);
            return new ESLicenseManager(ESLicenseProvider.createFileBasedLicenseProvider(merge(esLicensesSet), publicKeyPath));
        } else if (ESLicenseManager.instance.licenseProvider instanceof FileBasedESLicenseProvider) {
            return ESLicenseManager.instance;
        } else {
            throw new IllegalStateException("Manager already initiated with Cluster state based license provider");
        }
    }

    private static String getPublicKeyPath() {
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

    private ESLicenseManager(ESLicenseProvider licenseProvider) {
        LicenseManagerProperties.setLicenseProvider(licenseProvider);
        LicenseManagerProperties.setPublicKeyDataProvider(publicKeyDataProvider);
        LicenseManagerProperties.setLicenseValidator(new DefaultLicenseValidator());
        LicenseManagerProperties.setPublicKeyPasswordProvider(new ESPublicKeyPasswordProvider());
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
            for (FeatureType featureType : esLicenses.features()) {
                ESLicense esLicense = esLicenses.get(featureType);
                // verify signature
                final License license = this.licenseManager.decryptAndVerifyLicense(
                        extractSignedLicence(
                                esLicense.signature(),
                                publicKeyDataProvider.getPublicKeyFile().getAbsolutePath()));
                // validate license
                this.licenseManager.validateLicense(license);

                // verify all readable license fields
                verifyLicenseFields(license, esLicense);
            }
        } catch (ExpiredLicenseException e) {
            throw new InvalidLicenseException("Expired License");
        } catch (InvalidLicenseException e) {
            throw new InvalidLicenseException("Invalid License");
        } catch (IOException e) {
            // bogus
            throw new IllegalStateException(e);
        }
    }

    public void verifyLicenses() {
        verifyLicenses(getEffectiveLicenses());
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


    //TODO wrap License validation methods so a plugin does not have to provide featureType param

    public boolean hasLicenseForFeature(FeatureType featureType) {
        try {
            final License license = licenseManager.getLicense(featureType);
            if (license != null) {
                return license.hasLicenseForFeature(featureType.string());
            }
            return false;
        } catch (ExpiredLicenseException e) {
            return false;
        } catch (InvalidLicenseException e) {
            return false;
        }
    }

    public boolean hasLicenseForNodes(FeatureType featureType, int nodes) {
        ESLicense esLicense = getESLicense(featureType);
        return esLicense.maxNodes() >= nodes;
    }

    public String getIssuerForLicense(FeatureType featureType) {
        final License license = licenseManager.getLicense(featureType);
        return license.getIssuer();
    }

    public long getIssueDateForLicense(FeatureType featureType) {
        final License license = licenseManager.getLicense(featureType);
        return license.getIssueDate();
    }

    public long getExpiryDateForLicense(FeatureType featureType) {
        final License license = licenseManager.getLicense(featureType);
        return license.getGoodBeforeDate();
    }

    public String getIssuedToForLicense(FeatureType featureType) {
        final License license = licenseManager.getLicense(featureType);
        return license.getHolder();
    }

    public Type getTypeForLicense(FeatureType featureType) {
        ESLicense esLicense = getESLicense(featureType);
        return esLicense.type();
    }

    public SubscriptionType getSubscriptionTypeForLicense(FeatureType featureType) {
        ESLicense esLicense = getESLicense(featureType);
        return esLicense.subscriptionType();
    }

    ESLicense getESLicense(FeatureType featureType) {
        final License license = licenseManager.getLicense(featureType);
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


    // only for testing
    public void clearAndAddLicenses(ESLicenses licenses) {
        this.licenseManager.clearLicenseCache();
        assert this.licenseProvider instanceof FileBasedESLicenseProvider;
        this.licenseProvider.addLicenses(licenses);
    }

    private static class ESPublicKeyPasswordProvider implements PasswordProvider {
        private final String DEFAULT_PASS_PHRASE = "elasticsearch-license";

        @Override
        public char[] getPassword() {
            return Hasher.hash(DEFAULT_PASS_PHRASE).toCharArray();
        }
    }
}
