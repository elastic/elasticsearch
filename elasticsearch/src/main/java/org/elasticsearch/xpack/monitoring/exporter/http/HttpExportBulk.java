/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.ExportException;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;
import org.elasticsearch.xpack.monitoring.resolver.ResolversRegistry;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * {@code HttpExportBulk} uses the {@link RestClient} to perform a bulk operation against the remote cluster.
 */
class HttpExportBulk extends ExportBulk {

    private static final Logger logger = Loggers.getLogger(HttpExportBulk.class);

    /**
     * The {@link RestClient} managed by the {@link HttpExporter}.
     */
    private final RestClient client;

    /**
     * The querystring parameters to pass along with every bulk request.
     */
    private final Map<String, String> params;

    /**
     * Resolvers are used to render monitoring documents into JSON.
     */
    private final ResolversRegistry registry;

    /**
     * The bytes payload that represents the bulk body is created via {@link #doAdd(Collection)}.
     */
    private byte[] payload = null;

    HttpExportBulk(final String name, final RestClient client, final Map<String, String> parameters,
                          final ResolversRegistry registry) {
        super(name);

        this.client = client;
        this.params = parameters;
        this.registry = registry;
    }

    @Override
    public void doAdd(Collection<MonitoringDoc> docs) throws ExportException {
        try {
            if (docs != null && docs.isEmpty() == false) {
                try (BytesStreamOutput payload = new BytesStreamOutput()) {
                    for (MonitoringDoc monitoringDoc : docs) {
                        // any failure caused by an individual doc will be written as an empty byte[], thus not impacting the rest
                        payload.write(toBulkBytes(monitoringDoc));
                    }

                    // store the payload until we flush
                    this.payload = BytesReference.toBytes(payload.bytes());
                }
            }
        } catch (Exception e) {
            throw new ExportException("failed to add documents to export bulk [{}]", e, name);
        }
    }

    @Override
    public void doFlush() throws ExportException {
        if (payload == null) {
            throw new ExportException("unable to send documents because none were loaded for export bulk [{}]", name);
        } else if (payload.length != 0) {
            final HttpEntity body = new ByteArrayEntity(payload, ContentType.APPLICATION_JSON);

            client.performRequestAsync("POST", "/_bulk", params, body, HttpExportBulkResponseListener.INSTANCE);

            // free the memory
            payload = null;
        }
    }

    @Override
    protected void doClose() {
        // nothing serious to do at this stage
        assert payload == null;
    }

    private byte[] toBulkBytes(final MonitoringDoc doc) throws IOException {
        final XContentType xContentType = XContentType.JSON;
        final XContent xContent = xContentType.xContent();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            MonitoringIndexNameResolver<MonitoringDoc> resolver = registry.getResolver(doc);

            if (resolver != null) {
                String index = resolver.index(doc);
                String type = resolver.type(doc);
                String id = resolver.id(doc);

                try (XContentBuilder builder = new XContentBuilder(xContent, out)) {
                    // Builds the bulk action metadata line
                    builder.startObject();
                    builder.startObject("index");
                    builder.field("_index", index);
                    builder.field("_type", type);
                    if (id != null) {
                        builder.field("_id", id);
                    }
                    builder.endObject();
                    builder.endObject();
                }

                // Adds action metadata line bulk separator
                out.write(xContent.streamSeparator());

                // Render the monitoring document
                BytesRef bytesRef = resolver.source(doc, xContentType).toBytesRef();
                out.write(bytesRef.bytes, bytesRef.offset, bytesRef.length);

                // Adds final bulk separator
                out.write(xContent.streamSeparator());

                logger.trace("added index request [index={}, type={}, id={}]", index, type, id);
            } else {
                logger.error("no resolver found for monitoring document [class={}, id={}, version={}]",
                             doc.getClass().getName(), doc.getMonitoringId(), doc.getMonitoringVersion());
            }

            return BytesReference.toBytes(out.bytes());
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to render document [{}], skipping it [{}]", doc, name), e);

            return BytesRef.EMPTY_BYTES;
        }
    }

}
