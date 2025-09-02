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
public interface IngestDocumentBridge extends StableBridgeAPI<IngestDocument> {

    MetadataBridge getMetadata();

    Map<String, Object> getSource();

    boolean updateIndexHistory(String index);

    Set<String> getIndexHistory();

    boolean isReroute();

    void resetReroute();

    Map<String, Object> getIngestMetadata();

    <T> T getFieldValue(String name, Class<T> type);

    <T> T getFieldValue(String name, Class<T> type, boolean ignoreMissing);

    String renderTemplate(TemplateScriptBridge.Factory bridgedTemplateScriptFactory);

    void setFieldValue(String path, Object value);

    void removeField(String path);

    void executePipeline(PipelineBridge bridgedPipeline, BiConsumer<IngestDocumentBridge, Exception> bridgedHandler);

    final class Constants {
        public static final String METADATA_VERSION_FIELD_NAME = IngestDocument.Metadata.VERSION.getFieldName();

        private Constants() {}
    }

    static IngestDocumentBridge fromInternalNullable(final IngestDocument ingestDocument) {
        if (ingestDocument == null) {
            return null;
        }
        return new IngestDocumentBridge.ProxyInternal(ingestDocument);
    }

    static IngestDocumentBridge create(final Map<String, Object> sourceAndMetadata, final Map<String, Object> ingestMetadata) {
        final IngestDocument internal = new IngestDocument(sourceAndMetadata, ingestMetadata);
        return fromInternalNullable(internal);
    }

    class ProxyInternal extends StableBridgeAPI.ProxyInternal<IngestDocument> implements IngestDocumentBridge {

        private MetadataBridge metadataBridge;

        public ProxyInternal(final Map<String, Object> sourceAndMetadata, final Map<String, Object> ingestMetadata) {
            this(new IngestDocument(sourceAndMetadata, ingestMetadata));
        }

        private ProxyInternal(IngestDocument inner) {
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
        public String renderTemplate(final TemplateScriptBridge.Factory templateScriptFactory) {
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
}
