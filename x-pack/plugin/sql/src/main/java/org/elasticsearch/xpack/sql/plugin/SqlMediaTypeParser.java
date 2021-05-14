/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.common.xcontent.MediaType;
import org.elasticsearch.common.xcontent.MediaTypeRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.proto.Mode;


import static org.elasticsearch.xpack.sql.proto.Protocol.URL_PARAM_FORMAT;

public class SqlMediaTypeParser {
    public static final MediaTypeRegistry<? extends MediaType> MEDIA_TYPE_REGISTRY = new MediaTypeRegistry<>()
        .register(XContentType.values())
        .register(TextFormat.values());

    /*
     * Since we support {@link TextFormat} <strong>and</strong>
     * {@link XContent} outputs we can't use {@link RestToXContentListener}
     * like everything else. We want to stick as closely as possible to
     * Elasticsearch's defaults though, while still layering in ways to
     * control the output more easily.
     *
     * First we find the string that the user used to specify the response
     * format. If there is a {@code format} parameter we use that. If there
     * isn't but there is a {@code Accept} header then we use that. If there
     * isn't then we use the {@code Content-Type} header which is required.
     */
    public MediaType getResponseMediaType(RestRequest request, SqlQueryRequest sqlRequest) {
        if (Mode.isDedicatedClient(sqlRequest.requestInfo().mode())
            && (sqlRequest.binaryCommunication() == null || sqlRequest.binaryCommunication())) {
            // enforce CBOR response for drivers and CLI (unless instructed differently through the config param)
            return XContentType.CBOR;
        } else if (request.hasParam(URL_PARAM_FORMAT)) {
            return validateColumnarRequest(sqlRequest.columnar(),
                MEDIA_TYPE_REGISTRY.queryParamToMediaType(request.param(URL_PARAM_FORMAT)));
        }

        if (request.getParsedAccept() != null) {
            return request.getParsedAccept().toMediaType(MEDIA_TYPE_REGISTRY);
        }
        return request.getXContentType();
    }

    private static MediaType validateColumnarRequest(boolean requestIsColumnar, MediaType fromMediaType) {
        if (requestIsColumnar && fromMediaType instanceof TextFormat) {
            throw new IllegalArgumentException("Invalid use of [columnar] argument: cannot be used in combination with "
                + "txt, csv or tsv formats");
        }
        return fromMediaType;
    }
}
