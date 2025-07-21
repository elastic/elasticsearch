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

/**
 * An external bridge for {@link IngestDocument} that proxies calls through a real {@link IngestDocument}
 */
public class IngestDocumentBridge extends StableBridgeAPI.ProxyInternal<IngestDocument> {

    public static final class Constants {
        public static final String METADATA_VERSION_FIELD_NAME = IngestDocument.Metadata.VERSION.getFieldName();

        private Constants() {}
    }

    public static IngestDocumentBridge fromInternalNullable(final IngestDocument ingestDocument) {
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
        return new MetadataBridge(internalDelegate.getMetadata());
    }

    public Map<String, Object> getSource() {
        return internalDelegate.getSource();
    }

    public boolean updateIndexHistory(final String index) {
        return internalDelegate.updateIndexHistory(index);
    }

    public Set<String> getIndexHistory() {
        return Set.copyOf(internalDelegate.getIndexHistory());
    }

    public boolean isReroute() {
        return LogstashInternalBridge.isReroute(internalDelegate);
    }

    public void resetReroute() {
        LogstashInternalBridge.resetReroute(internalDelegate);
    }

    public Map<String, Object> getIngestMetadata() {
        return internalDelegate.getIngestMetadata();
    }

    public <T> T getFieldValue(final String fieldName, final Class<T> type) {
        return internalDelegate.getFieldValue(fieldName, type);
    }

    public <T> T getFieldValue(final String fieldName, final Class<T> type, final boolean ignoreMissing) {
        return internalDelegate.getFieldValue(fieldName, type, ignoreMissing);
    }

    public String renderTemplate(final TemplateScriptBridge.Factory templateScriptFactory) {
        return internalDelegate.renderTemplate(templateScriptFactory.toInternal());
    }

    public void setFieldValue(final String path, final Object value) {
        internalDelegate.setFieldValue(path, value);
    }

    public void removeField(final String path) {
        internalDelegate.removeField(path);
    }

    public void executePipeline(final PipelineBridge pipelineBridge, final BiConsumer<IngestDocumentBridge, Exception> handler) {
        this.internalDelegate.executePipeline(pipelineBridge.toInternal(),
                                              (ingestDocument, e) -> {
                                                  handler.accept(IngestDocumentBridge.fromInternalNullable(ingestDocument), e);
                                              });
    }
}
