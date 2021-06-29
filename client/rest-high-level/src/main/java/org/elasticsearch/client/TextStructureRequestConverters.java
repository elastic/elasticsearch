/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import static org.elasticsearch.client.RequestConverters.createContentType;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.client.RequestConverters.EndpointBuilder;
import org.elasticsearch.client.textstructure.FindStructureRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentType;

final class TextStructureRequestConverters {

    private TextStructureRequestConverters() {}

    static Request findFileStructure(FindStructureRequest findStructureRequest) {
        String endpoint = new EndpointBuilder()
            .addPathPartAsIs("_text_structure")
            .addPathPartAsIs("find_structure")
            .build();
        Request request = new Request(HttpPost.METHOD_NAME, endpoint);

        RequestConverters.Params params = new RequestConverters.Params();
        if (findStructureRequest.getLinesToSample() != null) {
            params.putParam(FindStructureRequest.LINES_TO_SAMPLE.getPreferredName(),
                findStructureRequest.getLinesToSample().toString());
        }
        if (findStructureRequest.getTimeout() != null) {
            params.putParam(FindStructureRequest.TIMEOUT.getPreferredName(), findStructureRequest.getTimeout().toString());
        }
        if (findStructureRequest.getCharset() != null) {
            params.putParam(FindStructureRequest.CHARSET.getPreferredName(), findStructureRequest.getCharset());
        }
        if (findStructureRequest.getFormat() != null) {
            params.putParam(FindStructureRequest.FORMAT.getPreferredName(), findStructureRequest.getFormat().toString());
        }
        if (findStructureRequest.getColumnNames() != null) {
            params.putParam(FindStructureRequest.COLUMN_NAMES.getPreferredName(),
                Strings.collectionToCommaDelimitedString(findStructureRequest.getColumnNames()));
        }
        if (findStructureRequest.getHasHeaderRow() != null) {
            params.putParam(FindStructureRequest.HAS_HEADER_ROW.getPreferredName(),
                findStructureRequest.getHasHeaderRow().toString());
        }
        if (findStructureRequest.getDelimiter() != null) {
            params.putParam(FindStructureRequest.DELIMITER.getPreferredName(),
                findStructureRequest.getDelimiter().toString());
        }
        if (findStructureRequest.getQuote() != null) {
            params.putParam(FindStructureRequest.QUOTE.getPreferredName(), findStructureRequest.getQuote().toString());
        }
        if (findStructureRequest.getShouldTrimFields() != null) {
            params.putParam(FindStructureRequest.SHOULD_TRIM_FIELDS.getPreferredName(),
                findStructureRequest.getShouldTrimFields().toString());
        }
        if (findStructureRequest.getGrokPattern() != null) {
            params.putParam(FindStructureRequest.GROK_PATTERN.getPreferredName(), findStructureRequest.getGrokPattern());
        }
        if (findStructureRequest.getTimestampFormat() != null) {
            params.putParam(FindStructureRequest.TIMESTAMP_FORMAT.getPreferredName(), findStructureRequest.getTimestampFormat());
        }
        if (findStructureRequest.getTimestampField() != null) {
            params.putParam(FindStructureRequest.TIMESTAMP_FIELD.getPreferredName(), findStructureRequest.getTimestampField());
        }
        if (findStructureRequest.getExplain() != null) {
            params.putParam(FindStructureRequest.EXPLAIN.getPreferredName(), findStructureRequest.getExplain().toString());
        }
        request.addParameters(params.asMap());
        BytesReference sample = findStructureRequest.getSample();
        BytesRef source = sample.toBytesRef();
        HttpEntity byteEntity = new NByteArrayEntity(source.bytes, source.offset, source.length, createContentType(XContentType.JSON));
        request.setEntity(byteEntity);
        return request;
    }
}
