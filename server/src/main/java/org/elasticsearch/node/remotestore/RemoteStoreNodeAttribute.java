/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.node.remotestore;


import org.elasticsearch.cluster.metadata.CryptoMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlags;
import org.elasticsearch.gateway.remote.RemoteClusterStateService;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;

/**
 * This is an abstraction for validating and storing information specific to remote backed storage nodes.
 *
 * @opensearch.internal
 */
public class RemoteStoreNodeAttribute {

    public static final String REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX = "remote_store";
    public static final String REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.segment.repository";
    public static final String REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.translog.repository";
    public static final String REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.state.repository";
    public static final String REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s.type";
    public static final String REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT = "remote_store.repository.%s."
        + CryptoMetadata.CRYPTO_METADATA_KEY;
    public static final String REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX = REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT
        + "."
        + CryptoMetadata.SETTINGS_KEY;
    public static final String REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX = "remote_store.repository.%s.settings.";
    public static final String REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY = "remote_store.routing_table.repository";

    private final RepositoriesMetadata repositoriesMetadata;

    public static List<String> SUPPORTED_DATA_REPO_NAME_ATTRIBUTES = List.of(
        REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
        REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY
    );

    /**
     * Creates a new {@link RemoteStoreNodeAttribute}
     */
    public RemoteStoreNodeAttribute(DiscoveryNode node) {
        this.repositoriesMetadata = buildRepositoriesMetadata(node);
    }

    private String validateAttributeNonNull(DiscoveryNode node, String attributeKey) {
        String attributeValue = node.getAttributes().get(attributeKey);
        if (attributeValue == null || attributeValue.isEmpty()) {
            throw new IllegalStateException("joining node [" + node + "] doesn't have the node attribute [" + attributeKey + "]");
        }

        return attributeValue;
    }

    private CryptoMetadata buildCryptoMetadata(DiscoveryNode node, String repositoryName) {
        String metadataKey = String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_CRYPTO_ATTRIBUTE_KEY_FORMAT, repositoryName);
        boolean isRepoEncrypted = node.getAttributes().keySet().stream().anyMatch(key -> key.startsWith(metadataKey));
        if (isRepoEncrypted == false) {
            return null;
        }

        String keyProviderName = validateAttributeNonNull(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_NAME_KEY);
        String keyProviderType = validateAttributeNonNull(node, metadataKey + "." + CryptoMetadata.KEY_PROVIDER_TYPE_KEY);

        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_CRYPTO_SETTINGS_PREFIX,
            repositoryName
        );

        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix + ".", ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        return new CryptoMetadata(keyProviderName, keyProviderType, settings.build());
    }

    private Map<String, String> validateSettingsAttributesNonNull(DiscoveryNode node, String repositoryName) {
        String settingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            repositoryName
        );
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> validateAttributeNonNull(node, key)));

        if (settingsMap.isEmpty()) {
            throw new IllegalStateException(
                "joining node [" + node + "] doesn't have settings attribute for [" + repositoryName + "] repository"
            );
        }

        return settingsMap;
    }

    private RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        String type = validateAttributeNonNull(
            node,
            String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name)
        );
        Map<String, String> settingsMap = validateSettingsAttributesNonNull(node, name);

        Settings.Builder settings = Settings.builder();
        settingsMap.forEach(settings::put);

        CryptoMetadata cryptoMetadata = buildCryptoMetadata(node, name);

        // Repository metadata built here will always be for a system repository.
        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build(), cryptoMetadata);
    }

    private RepositoriesMetadata buildRepositoriesMetadata(DiscoveryNode node) {
        Set<String> repositoryNames = getValidatedRepositoryNames(node);
        List<RepositoryMetadata> repositoryMetadataList = new ArrayList<>();

        for (String repositoryName : repositoryNames) {
            repositoryMetadataList.add(buildRepositoryMetadata(node, repositoryName));
        }

        return new RepositoriesMetadata(repositoryMetadataList);
    }

    private Set<String> getValidatedRepositoryNames(DiscoveryNode node) {
        Set<String> repositoryNames = new HashSet<>();
        if (node.getAttributes().containsKey(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY)
            || node.getAttributes().containsKey(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY)) {
            repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY));
            repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY));
            repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY));
        } else if (node.getAttributes().containsKey(REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY)) {
            repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY));
        }
        if (node.getAttributes().containsKey(REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY)) {
            repositoryNames.add(validateAttributeNonNull(node, REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY));
        }

        return repositoryNames;
    }

    public static boolean isRemoteStoreAttributePresent(Settings settings) {
        return settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX).isEmpty() == false;
    }

    public static boolean isRemoteDataAttributePresent(Settings settings) {
        return settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY).isEmpty() == false
            || settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY).isEmpty() == false;
    }

    public static boolean isRemoteClusterStateAttributePresent(Settings settings) {
        return settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY)
            .isEmpty() == false;
    }

    public static String getRemoteStoreSegmentRepo(Settings settings) {
        return settings.get(Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY);
    }

    public static String getRemoteStoreTranslogRepo(Settings settings) {
        return settings.get(Node.NODE_ATTRIBUTES.getKey() + RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY);
    }

    public static boolean isRemoteStoreClusterStateEnabled(Settings settings) {
        return RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.get(settings)
            && isRemoteClusterStateAttributePresent(settings);
    }

    private static boolean isRemoteRoutingTableAttributePresent(Settings settings) {
        return settings.getByPrefix(Node.NODE_ATTRIBUTES.getKey() + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY)
            .isEmpty() == false;
    }

    public static boolean isRemoteRoutingTableEnabled(Settings settings) {
        return FeatureFlags.isEnabled(REMOTE_PUBLICATION_EXPERIMENTAL) && isRemoteRoutingTableAttributePresent(settings);
    }

    public RepositoriesMetadata getRepositoriesMetadata() {
        return this.repositoriesMetadata;
    }

    /**
     * Return {@link Map} of all the supported data repo names listed on {@link RemoteStoreNodeAttribute#SUPPORTED_DATA_REPO_NAME_ATTRIBUTES}
     *
     * @param node Node to fetch attributes from
     * @return {@link Map} of all remote store data repo attribute keys and their values
     */
    public static Map<String, String> getDataRepoNames(DiscoveryNode node) {
        assert remoteDataAttributesPresent(node.getAttributes());
        Map<String, String> dataRepoNames = new HashMap<>();
        for (String supportedRepoAttribute : SUPPORTED_DATA_REPO_NAME_ATTRIBUTES) {
            dataRepoNames.put(supportedRepoAttribute, node.getAttributes().get(supportedRepoAttribute));
        }
        return dataRepoNames;
    }

    private static boolean remoteDataAttributesPresent(Map<String, String> nodeAttrs) {
        for (String supportedRepoAttributes : SUPPORTED_DATA_REPO_NAME_ATTRIBUTES) {
            if (nodeAttrs.get(supportedRepoAttributes) == null || nodeAttrs.get(supportedRepoAttributes).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // The hashCode is generated by computing the hash of all the repositoryMetadata present in
        // repositoriesMetadata without generation. Below is the modified list hashCode generation logic.

        int hashCode = 1;
        Iterator iterator = this.repositoriesMetadata.repositories().iterator();
        while (iterator.hasNext()) {
            RepositoryMetadata repositoryMetadata = (RepositoryMetadata) iterator.next();
            hashCode = 31 * hashCode + (repositoryMetadata == null
                ? 0
                : Objects.hash(repositoryMetadata.name(), repositoryMetadata.type(), repositoryMetadata.settings()));
        }
        return hashCode;
    }

    /**
     * Checks if 2 instances are equal, with option to skip check for a list of repos.
     * *
     * @param o other instance
     * @param reposToSkip list of repos to skip check for equality
     * @return {@code true} iff both instances are equal, not including the repositories in both instances if they are part of reposToSkip.
     */
    public boolean equalsWithRepoSkip(Object o, List<String> reposToSkip) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNodeAttribute that = (RemoteStoreNodeAttribute) o;
        return this.getRepositoriesMetadata().equalsIgnoreGenerationsWithRepoSkip(that.getRepositoriesMetadata(), reposToSkip);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RemoteStoreNodeAttribute that = (RemoteStoreNodeAttribute) o;

        return this.getRepositoriesMetadata().equalsIgnoreGenerations(that.getRepositoriesMetadata());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('{').append(this.repositoriesMetadata).append('}');
        return super.toString();
    }
}
