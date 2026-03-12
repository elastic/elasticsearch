/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.logstashbridge.ingest;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.LogstashInternalBridge;
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.script.MetadataBridge;
import org.elasticsearch.logstashbridge.script.TemplateScriptFactoryBridge;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * An implementation of {@link IngestDocumentBridge} that proxies calls through
 * to an internal {@link IngestDocument}.
 * @see StableBridgeAPI.ProxyInternal
 */
final class ProxyInternalIngestDocumentBridge extends StableBridgeAPI.ProxyInternal<IngestDocument> implements IngestDocumentBridge {

    private MetadataBridge metadataBridge;

    ProxyInternalIngestDocumentBridge(IngestDocument inner) {
        super(inner);
    }

    @Override
    public MetadataBridge getMetadata() {
        if (metadataBridge == null) {
            this.metadataBridge = MetadataBridge.fromInternal(internalDelegate.getMetadata());
        }
        return this.metadataBridge;
    }

    @Override
    public Map<String, Object> getSource() {
        return internalDelegate.getSource();
    }

    @Override
    public boolean updateIndexHistory(final String index) {
        return internalDelegate.updateIndexHistory(index);
    }

    @Override
    public Set<String> getIndexHistory() {
        return Set.copyOf(internalDelegate.getIndexHistory());
    }

    @Override
    public boolean isReroute() {
        return LogstashInternalBridge.isReroute(internalDelegate);
    }

    @Override
    public void resetReroute() {
        LogstashInternalBridge.resetReroute(internalDelegate);
    }

    @Override
    public Map<String, Object> getIngestMetadata() {
        return internalDelegate.getIngestMetadata();
    }

    @Override
    public <T> T getFieldValue(final String fieldName, final Class<T> type) {
        return internalDelegate.getFieldValue(fieldName, type);
    }

    @Override
    public <T> T getFieldValue(final String fieldName, final Class<T> type, final boolean ignoreMissing) {
        return internalDelegate.getFieldValue(fieldName, type, ignoreMissing);
    }

    @Override
    public String renderTemplate(final TemplateScriptFactoryBridge templateScriptFactory) {
        return internalDelegate.renderTemplate(templateScriptFactory.toInternal());
    }

    @Override
    public void setFieldValue(final String path, final Object value) {
        internalDelegate.setFieldValue(path, value);
    }

    @Override
    public void removeField(final String path) {
        internalDelegate.removeField(path);
    }

    @Override
    public void executePipeline(final PipelineBridge pipelineBridge, final BiConsumer<IngestDocumentBridge, Exception> handler) {
        this.internalDelegate.executePipeline(pipelineBridge.toInternal(), (ingestDocument, e) -> {
            handler.accept(IngestDocumentBridge.fromInternalNullable(ingestDocument), e);
        });
    }
}
