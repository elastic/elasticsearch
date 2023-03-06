/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.rest.ChunkedRestResponseBody;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xpack.esql.formatter.TextFormat;
import org.elasticsearch.xpack.esql.plugin.EsqlMediaTypeParser;

import java.util.Locale;

import static org.elasticsearch.xpack.esql.formatter.TextFormat.CSV;
import static org.elasticsearch.xpack.esql.formatter.TextFormat.URL_PARAM_DELIMITER;

public class EsqlResponseListener extends RestResponseListener<EsqlQueryResponse> {

    private final RestChannel channel;
    private final RestRequest restRequest;
    private final MediaType mediaType;
    private final long startNanos = System.nanoTime();
    private static final String HEADER_NAME_TOOK_NANOS = "Took-nanos";

    public EsqlResponseListener(RestChannel channel, RestRequest restRequest, EsqlQueryRequest esqlRequest) {
        super(channel);

        this.channel = channel;
        this.restRequest = restRequest;
        mediaType = EsqlMediaTypeParser.getResponseMediaType(restRequest, esqlRequest);

        /*
         * Special handling for the "delimiter" parameter which should only be
         * checked for being present or not in the case of CSV format. We cannot
         * override {@link BaseRestHandler#responseParams()} because this
         * parameter should only be checked for CSV, not other formats.
         */
        if (mediaType != CSV && restRequest.hasParam(URL_PARAM_DELIMITER)) {
            String message = String.format(
                Locale.ROOT,
                "parameter: [%s] can only be used with the format [%s] for request [%s]",
                URL_PARAM_DELIMITER,
                CSV.queryParameter(),
                restRequest.path()
            );
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public RestResponse buildResponse(EsqlQueryResponse esqlResponse) throws Exception {
        RestResponse restResponse;
        if (mediaType instanceof TextFormat format) {
            restResponse = new RestResponse(RestStatus.OK, format.contentType(restRequest), format.format(restRequest, esqlResponse));
        } else {
            restResponse = new RestResponse(RestStatus.OK, ChunkedRestResponseBody.fromXContent(esqlResponse, channel.request(), channel));
        }
        restResponse.addHeader(HEADER_NAME_TOOK_NANOS, Long.toString(System.nanoTime() - startNanos));

        return restResponse;
    }
}
