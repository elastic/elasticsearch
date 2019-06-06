/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.exporter.http;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Objects;

/**
 * {@code HttpExportBulkResponseListener} logs issues based on the response, but otherwise does nothing else.
 */
class HttpExportBulkResponseListener implements ResponseListener {

    private static final Logger logger = LogManager.getLogger(HttpExportBulkResponseListener.class);

    /**
     * Singleton instance.
     */
    public static final HttpExportBulkResponseListener INSTANCE = new HttpExportBulkResponseListener(XContentType.JSON.xContent());

    /**
     * The response content type.
     */
    private final XContent xContent;

    /**
     * Create a new {@link HttpExportBulkResponseListener}.
     *
     * @param xContent The {@code XContent} to use for parsing the response.
     */
    HttpExportBulkResponseListener(final XContent xContent) {
        this.xContent = Objects.requireNonNull(xContent);
    }

    /**
     * Success is relative with bulk responses because unless it's rejected outright, it returns with a 200.
     * <p>
     * Individual documents can fail and since we know how we're making them, that means that .
     */
    @Override
    public void onSuccess(final Response response) {
        // EMPTY is safe here because we never call namedObject
        try (XContentParser parser = xContent
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getEntity().getContent())) {
            // avoid parsing the entire payload if we don't need too
            XContentParser.Token token = parser.nextToken();

            if (token == XContentParser.Token.START_OBJECT) {
                String currentFieldName = null;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if ("errors".equals(currentFieldName)) {
                            // no errors? then we can stop looking
                            if (parser.booleanValue() == false) {
                                return;
                            }
                        }
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        // note: this assumes that "items" is the only array portion of the response (currently true)
                        parseErrors(parser);
                        return;
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            onError("unexpected exception while verifying bulk response", e);
        }
    }

    /**
     * Logs every <code>error</code> field's value until it hits the end of an array.
     *
     * @param parser The bulk response parser
     * @throws IOException if any parsing error occurs
     */
    private void parseErrors(final XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("error".equals(currentFieldName)) {
                    onItemError(parser.text());
                }
            }
        }
    }

    /**
     * Log obvious failures.
     * <p>
     * In the future, we should queue replayable failures.
     */
    @Override
    public void onFailure(final Exception exception) {
        // queueable exceptions:
        // - RestStatus.TOO_MANY_REQUESTS.getStatus()
        // - possibly other, non-ResponseExceptions
        onError("bulk request failed unexpectedly", exception);
    }

    void onError(final String msg, final Throwable cause) {
        logger.warn(msg, cause);
    }

    void onItemError(final String text) {
        logger.warn("unexpected error while indexing monitoring document: [{}]", text);
    }

}
