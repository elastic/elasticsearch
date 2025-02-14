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
import org.elasticsearch.logstashbridge.script.TemplateScriptBridge;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

public class IngestDocumentBridge extends StableBridgeAPI.Proxy<IngestDocument> {

    public static IngestDocumentBridge wrap(final IngestDocument ingestDocument) {
        if (ingestDocument == null) {
            return null;
        }
        return new IngestDocumentBridge(ingestDocument);
    }

    public IngestDocumentBridge(final Map<String, Object> sourceAndMetadata, final Map<String, Object> ingestMetadata) {
        this(new IngestDocument(sourceAndMetadata, ingestMetadata));
    }

    private IngestDocumentBridge(IngestDocument inner) {
        super(inner);
    }

    public MetadataBridge getMetadata() {
        return new MetadataBridge(delegate.getMetadata());
    }

    public Map<String, Object> getSource() {
        return delegate.getSource();
    }

    public boolean updateIndexHistory(final String index) {
        return delegate.updateIndexHistory(index);
    }

    public Set<String> getIndexHistory() {
        return Set.copyOf(delegate.getIndexHistory());
    }

    public boolean isReroute() {
        return LogstashInternalBridge.isReroute(delegate);
    }

    public void resetReroute() {
        LogstashInternalBridge.resetReroute(delegate);
    }

    public Map<String, Object> getIngestMetadata() {
        return Map.copyOf(delegate.getIngestMetadata());
    }

    public <T> T getFieldValue(final String fieldName, final Class<T> type) {
        return delegate.getFieldValue(fieldName, type);
    }

    public <T> T getFieldValue(final String fieldName, final Class<T> type, final boolean ignoreMissing) {
        return delegate.getFieldValue(fieldName, type, ignoreMissing);
    }

    public String renderTemplate(final TemplateScriptBridge.Factory templateScriptFactory) {
        return delegate.renderTemplate(templateScriptFactory.unwrap());
    }

    public void setFieldValue(final String path, final Object value) {
        delegate.setFieldValue(path, value);
    }

    public void removeField(final String path) {
        delegate.removeField(path);
    }

    // public void executePipeline(Pipeline pipeline, BiConsumer<IngestDocument, Exception> handler) {
    public void executePipeline(final PipelineBridge pipelineBridge, final BiConsumer<IngestDocumentBridge, Exception> handler) {
        this.delegate.executePipeline(pipelineBridge.unwrap(), (unwrapped, e) -> handler.accept(IngestDocumentBridge.wrap(unwrapped), e));
    }
}
