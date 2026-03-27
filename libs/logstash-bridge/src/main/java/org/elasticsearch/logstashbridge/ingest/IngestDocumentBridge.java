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
import org.elasticsearch.logstashbridge.StableBridgeAPI;
import org.elasticsearch.logstashbridge.script.MetadataBridge;
import org.elasticsearch.logstashbridge.script.TemplateScriptFactoryBridge;

import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A {@link StableBridgeAPI} for {@link IngestDocument}.
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

    String renderTemplate(TemplateScriptFactoryBridge bridgedTemplateScriptFactory);

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
        return new ProxyInternalIngestDocumentBridge(ingestDocument);
    }

    static IngestDocumentBridge create(final Map<String, Object> sourceAndMetadata, final Map<String, Object> ingestMetadata) {
        final IngestDocument internal = new IngestDocument(sourceAndMetadata, ingestMetadata);
        return fromInternalNullable(internal);
    }

}
