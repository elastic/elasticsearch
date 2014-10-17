/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.manager;

import net.nicholaswilliams.java.licensing.LicenseProvider;
import net.nicholaswilliams.java.licensing.ObjectSerializer;
import net.nicholaswilliams.java.licensing.SignedLicense;
import net.nicholaswilliams.java.licensing.encryption.Hasher;
import net.nicholaswilliams.java.licensing.exception.InvalidLicenseException;
import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.elasticsearch.license.plugin.core.LicensesMetaData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.elasticsearch.license.core.ESLicenses.ESLicense;
import static org.elasticsearch.license.core.ESLicenses.FeatureType;

public abstract class ESLicenseProvider implements LicenseProvider {

    /**
     * Factory for {@link org.elasticsearch.license.manager.ESLicenseProvider.ClusterStateLicenseProvider}
     */
    public static ESLicenseProvider createClusterBasedLicenseProvider(ClusterService clusterService, String publicKeyPath) {
        return new ClusterStateLicenseProvider(clusterService, publicKeyPath);
    }

    /**
     * Factory for {@link org.elasticsearch.license.manager.ESLicenseProvider.FileBasedESLicenseProvider}
     */
    public static ESLicenseProvider createFileBasedLicenseProvider(ESLicenses esLicenses, String publicKeyPath) {
        return new FileBasedESLicenseProvider(esLicenses, publicKeyPath);
    }

    public abstract ESLicense getESLicense(FeatureType featureType);

    public abstract ESLicenses getEffectiveLicenses();

    // only for testing
    public abstract void addLicenses(ESLicenses esLicenses);

    /**
     * LicenseProvider backed by the current cluster state
     * Uses the clusterService to query for the latest licenses
     * for {@link org.elasticsearch.license.manager.ESLicenseManager}
     * consumption
     */
    public static class ClusterStateLicenseProvider extends ESLicenseProvider {
        private final ClusterService clusterService;
        private final String publicKeyPath;

        private ClusterStateLicenseProvider(ClusterService clusterService, String publicKeyPath) {
            this.clusterService = clusterService;
            this.publicKeyPath = publicKeyPath;
        }

        private ESLicenses getLicensesFromClusterState() {
            final ClusterState state = clusterService.state();
            LicensesMetaData metaData = state.metaData().custom(LicensesMetaData.TYPE);
            if (metaData != null) {
                return Utils.fromSignatures(metaData.getSignatures());
            }
            return LicenseBuilders.licensesBuilder().build();
        }


        public ESLicenses getESLicenses() {
            return getLicensesFromClusterState();
        }

        @Override
        public SignedLicense getLicense(Object context) {
            assert context instanceof FeatureType;
            FeatureType featureType = (FeatureType) context;
            final ESLicenses licenses = getLicensesFromClusterState();
            final ESLicense esLicense = licenses.get(featureType);
            if (esLicense == null) {
                throw new InvalidLicenseException("Invalid License");
            }
            try {
                return extractSignedLicence(esLicense.signature(), publicKeyPath);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public ESLicense getESLicense(FeatureType featureType) {
            return getLicensesFromClusterState().get(featureType);
        }

        @Override
        public ESLicenses getEffectiveLicenses() {
            return getLicensesFromClusterState();
        }

        @Override
        public void addLicenses(ESLicenses esLicenses) {
            throw new UnsupportedOperationException("Licenses can only be added by updating the cluster state");
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
     * @param publicKeyPath location of the public key used to decode the signature
     * @return signed license content for the license
     * @throws IOException
     */
    static SignedLicense extractSignedLicence(String signature, String publicKeyPath) throws IOException {
        byte[] signatureBytes = Base64.decodeBase64(signature);
        ByteBuffer byteBuffer = ByteBuffer.wrap(signatureBytes);
        byteBuffer = (ByteBuffer) byteBuffer.position(13);
        int start = byteBuffer.getInt();
        int version = byteBuffer.getInt();
        byte[] hash = new byte[start - 13 - 4 - 4];
        byteBuffer.get(hash);

        final byte[] computedHash = Hasher.hash(Base64.encodeBase64String(
                Files.readAllBytes(Paths.get(publicKeyPath))
        )).getBytes(Charset.forName("UTF-8"));

        if (!Arrays.equals(hash, computedHash)) {
            throw new InvalidLicenseException("Invalid License");
        }

        return new ObjectSerializer().readObject(SignedLicense.class, Arrays.copyOfRange(signatureBytes, start, signatureBytes.length));
    }

    /**
     * Not thread-safe (allows setting licenses); used only for testing and in command-line tools
     */
    public static class FileBasedESLicenseProvider extends ESLicenseProvider {
        private ESLicenses esLicenses;
        private final String publicKeyPath;

        private FileBasedESLicenseProvider(ESLicenses esLicenses, String publicKeyPath) {
            this.esLicenses = esLicenses;
            this.publicKeyPath = publicKeyPath;
        }

        @Override
        public SignedLicense getLicense(Object context) {
            assert context instanceof FeatureType;
            FeatureType featureType = (FeatureType) context;
            ESLicense esLicense = esLicenses.get(featureType);
            if (esLicense == null) {
                throw new InvalidLicenseException("Invalid License");
            }
            try {
                return extractSignedLicence(esLicense.signature(), publicKeyPath);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public ESLicense getESLicense(FeatureType featureType) {
            return esLicenses.get(featureType);
        }

        @Override
        public ESLicenses getEffectiveLicenses() {
            return esLicenses;
        }

        @Override
        public void addLicenses(ESLicenses esLicenses) {
            this.esLicenses = esLicenses;
        }
    }
}
