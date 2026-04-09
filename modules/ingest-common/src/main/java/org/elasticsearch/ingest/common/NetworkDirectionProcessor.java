/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.network.NetworkDirectionUtils;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.TemplateScript;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;

public class NetworkDirectionProcessor extends AbstractProcessor {

    public static final String TYPE = "network_direction";

    private final String sourceIpField;
    private final String destinationIpField;
    private final String targetField;
    private final List<TemplateScript.Factory> internalNetworks;
    private final String internalNetworksField;
    private final boolean ignoreMissing;

    NetworkDirectionProcessor(
        String tag,
        String description,
        String sourceIpField,
        String destinationIpField,
        String targetField,
        List<TemplateScript.Factory> internalNetworks,
        String internalNetworksField,
        boolean ignoreMissing
    ) {
        super(tag, description);
        this.sourceIpField = sourceIpField;
        this.destinationIpField = destinationIpField;
        this.targetField = targetField;
        this.internalNetworks = internalNetworks;
        this.internalNetworksField = internalNetworksField;
        this.ignoreMissing = ignoreMissing;
    }

    public String getSourceIpField() {
        return sourceIpField;
    }

    public String getDestinationIpField() {
        return destinationIpField;
    }

    public String getTargetField() {
        return targetField;
    }

    public List<TemplateScript.Factory> getInternalNetworks() {
        return internalNetworks;
    }

    public String getInternalNetworksField() {
        return internalNetworksField;
    }

    public boolean getIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String direction = getDirection(ingestDocument);
        if (direction == null) {
            if (ignoreMissing) {
                return ingestDocument;
            } else {
                throw new IllegalArgumentException("unable to calculate network direction from document");
            }
        }

        ingestDocument.setFieldValue(targetField, direction);
        return ingestDocument;
    }

    private String getDirection(IngestDocument d) throws Exception {
        List<String> networks = new ArrayList<>();

        if (internalNetworksField != null) {
            @SuppressWarnings("unchecked")
            List<String> stringList = d.getFieldValue(internalNetworksField, networks.getClass(), ignoreMissing);
            if (stringList == null) {
                return null;
            }
            networks.addAll(stringList);
        } else {
            networks = internalNetworks.stream().map(network -> d.renderTemplate(network)).toList();
        }

        String sourceIpAddrString = d.getFieldValue(sourceIpField, String.class, ignoreMissing);
        if (sourceIpAddrString == null) {
            return null;
        }

        String destIpAddrString = d.getFieldValue(destinationIpField, String.class, ignoreMissing);
        if (destIpAddrString == null) {
            return null;
        }

        boolean sourceInternal = NetworkDirectionUtils.isInternal(networks, sourceIpAddrString);
        boolean destinationInternal = NetworkDirectionUtils.isInternal(networks, destIpAddrString);

        return NetworkDirectionUtils.getDirection(sourceInternal, destinationInternal);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {
        private final ScriptService scriptService;
        static final String DEFAULT_SOURCE_IP = "source.ip";
        static final String DEFAULT_DEST_IP = "destination.ip";
        static final String DEFAULT_TARGET = "network.direction";

        public Factory(ScriptService scriptService) {
            this.scriptService = scriptService;
        }

        @Override
        public NetworkDirectionProcessor create(
            Map<String, Processor.Factory> registry,
            String processorTag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            final String sourceIpField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "source_ip", DEFAULT_SOURCE_IP);
            final String destIpField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "destination_ip", DEFAULT_DEST_IP);
            final String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", DEFAULT_TARGET);
            final boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);

            final List<String> internalNetworks = ConfigurationUtils.readOptionalList(TYPE, processorTag, config, "internal_networks");
            final String internalNetworksField = ConfigurationUtils.readOptionalStringProperty(
                TYPE,
                processorTag,
                config,
                "internal_networks_field"
            );

            if (internalNetworks == null && internalNetworksField == null) {
                throw newConfigurationException(TYPE, processorTag, "internal_networks", "or [internal_networks_field] must be specified");
            }
            if (internalNetworks != null && internalNetworksField != null) {
                throw newConfigurationException(
                    TYPE,
                    processorTag,
                    "internal_networks",
                    "and [internal_networks_field] cannot both be used in the same processor"
                );
            }

            List<TemplateScript.Factory> internalNetworkTemplates = null;
            if (internalNetworks != null) {
                internalNetworkTemplates = internalNetworks.stream()
                    .map(n -> ConfigurationUtils.compileTemplate(TYPE, processorTag, "internal_networks", n, scriptService))
                    .toList();
            }
            return new NetworkDirectionProcessor(
                processorTag,
                description,
                sourceIpField,
                destIpField,
                targetField,
                internalNetworkTemplates,
                internalNetworksField,
                ignoreMissing
            );
        }
    }
}
