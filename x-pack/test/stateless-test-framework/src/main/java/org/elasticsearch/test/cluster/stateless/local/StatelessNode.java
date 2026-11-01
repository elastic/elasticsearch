/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.test.cluster.local.AbstractLocalClusterFactory;
import org.elasticsearch.test.cluster.local.LocalClusterSpec.LocalNodeSpec;
import org.elasticsearch.test.cluster.local.distribution.DistributionResolver;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.MutableResource;
import org.elasticsearch.test.cluster.util.resource.Resource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * A node for stateless test clusters that writes secure settings as cluster secrets into
 * {@code operator/settings.json} instead of using the Elasticsearch keystore.
 */
public class StatelessNode extends AbstractLocalClusterFactory.Node {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String OPERATOR_SETTINGS_FILE = "operator/settings.json";

    public StatelessNode(Path baseWorkingDir, DistributionResolver distributionResolver, LocalNodeSpec spec) {
        super(baseWorkingDir, distributionResolver, adaptStatelessNodeSpec(spec), RandomStringUtils.randomAlphabetic(7));
    }

    @Override
    public void updateStoredSecureSettings() {
        throw new UnsupportedOperationException("updating stored secure settings is not supported in stateless test clusters");
    }

    @Override
    public void configureKeystore() {
        // In stateless mode, secure settings are written as cluster secrets into operator/settings.json
        // rather than the Elasticsearch keystore. This is handled by adaptStatelessNodeSpec.
    }

    private static LocalNodeSpec adaptStatelessNodeSpec(LocalNodeSpec spec) {
        if (spec.getKeystoreFiles().isEmpty() == false) {
            throw new IllegalStateException(
                "Non-string secure secrets are not supported in stateless. Secrets: ["
                    + String.join(",", spec.getKeystoreFiles().keySet())
                    + "]"
            );
        }

        Map<String, String> secrets = spec.resolveKeystore();
        if (secrets.isEmpty()) {
            return spec;
        }

        Map<String, Resource> configFiles = new HashMap<>(spec.getExtraConfigFiles());
        configFiles.compute(OPERATOR_SETTINGS_FILE, (key, settings) -> {
            JsonNode baseSettings = baseSettingsWithSecrets(spec.getVersion(), secrets);
            if (settings instanceof MutableResource mutable) {
                MutableResource merged = MutableResource.from(mergeSettings(baseSettings.deepCopy(), settings));
                mutable.addUpdateListener(updated -> merged.update(mergeSettings(baseSettings.deepCopy(), updated)));
                return merged;
            }
            return settings == null ? Resource.fromString(baseSettings.toString()) : mergeSettings(baseSettings, settings);

        });
        return spec.copyWithExtraConfigFiles(configFiles);
    }

    private static ObjectNode baseSettingsWithSecrets(Version version, Map<String, String> secrets) {
        return MAPPER.createObjectNode().setAll(Map.of("metadata", metadata(version), "state", clusterSecretsState(secrets)));
    }

    private static JsonNode metadata(Version version) {
        return MAPPER.createObjectNode()
            .setAll(Map.of("version", TextNode.valueOf("1"), "compatibility", TextNode.valueOf(version.toString())));
    }

    private static JsonNode clusterSecretsState(Map<String, String> secrets) {
        ObjectNode stringSecrets = MAPPER.createObjectNode();
        secrets.forEach((k, v) -> stringSecrets.set(k, TextNode.valueOf(v)));
        return MAPPER.createObjectNode().set("cluster_secrets", MAPPER.createObjectNode().set("string_secrets", stringSecrets));
    }

    /**
     * Merge the overlay settings into the base settings that contain credentials.
     */
    private static Resource mergeSettings(JsonNode baseSettings, Resource overlaySettings) {
        try {
            JsonNode merged = MAPPER.readerForUpdating(baseSettings).readValue(overlaySettings.asStream());
            return Resource.fromString(merged.toString());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
