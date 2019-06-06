/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringTemplateUtils;
import org.elasticsearch.xpack.monitoring.exporter.ExportBulk;
import org.elasticsearch.xpack.monitoring.exporter.ExportException;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Map;

/**
 * {@code HttpExportBulk} uses the {@link RestClient} to perform a bulk operation against the remote cluster.
 */
class HttpExportBulk extends ExportBulk {

    private static final Logger logger = LogManager.getLogger(HttpExportBulk.class);

    /**
     * The {@link RestClient} managed by the {@link HttpExporter}.
     */
    private final RestClient client;

    /**
     * The querystring parameters to pass along with every bulk request.
     */
    private final Map<String, String> params;

    /**
     * {@link DateTimeFormatter} used to resolve timestamped index name.
     */
    private final DateFormatter formatter;

    /**
     * The bytes payload that represents the bulk body is created via {@link #doAdd(Collection)}.
     */
    private byte[] payload = null;

    HttpExportBulk(final String name, final RestClient client, final Map<String, String> parameters,
                   final DateFormatter dateTimeFormatter, final ThreadContext threadContext) {
        super(name, threadContext);

        this.client = client;
        this.params = parameters;
        this.formatter = dateTimeFormatter;
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
    public void doFlush(ActionListener<Void> listener) throws ExportException {
        if (payload == null) {
            listener.onFailure(new ExportException("unable to send documents because none were loaded for export bulk [{}]", name));
        } else if (payload.length != 0) {
            final Request request = new Request("POST", "/_bulk");
            for (Map.Entry<String, String> param : params.entrySet()) {
                request.addParameter(param.getKey(), param.getValue());
            }
            request.setEntity(new NByteArrayEntity(payload, ContentType.APPLICATION_JSON));

            client.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    try {
                        HttpExportBulkResponseListener.INSTANCE.onSuccess(response);
                    } finally {
                        listener.onResponse(null);
                    }
                }

                @Override
                public void onFailure(Exception exception) {
                    try {
                        HttpExportBulkResponseListener.INSTANCE.onFailure(exception);
                    } finally {
                        listener.onFailure(exception);
                    }
                }
            });
        }
    }

    private byte[] toBulkBytes(final MonitoringDoc doc) throws IOException {
        final XContentType xContentType = XContentType.JSON;
        final XContent xContent = xContentType.xContent();

        final String index = MonitoringTemplateUtils.indexName(formatter, doc.getSystem(), doc.getTimestamp());
        final String id = doc.getId();

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            try (XContentBuilder builder = new XContentBuilder(xContent, out)) {
                // Builds the bulk action metadata line
                builder.startObject();
                {
                    builder.startObject("index");
                    {
                        builder.field("_index", index);
                        if (id != null) {
                            builder.field("_id", id);
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }

            // Adds action metadata line bulk separator
            out.write(xContent.streamSeparator());

            // Adds the source of the monitoring document
            final BytesRef source = XContentHelper.toXContent(doc, xContentType, false).toBytesRef();
            out.write(source.bytes, source.offset, source.length);

            // Adds final bulk separator
            out.write(xContent.streamSeparator());

            logger.trace(
                "http exporter [{}] - added index request [index={}, id={}, monitoring data type={}]",
                name, index, id, doc.getType()
            );

            return BytesReference.toBytes(out.bytes());
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to render document [{}], skipping it [{}]", doc, name), e);

            return BytesRef.EMPTY_BYTES;
        }
    }

}
