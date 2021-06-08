/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.XContentUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class ExecuteWatchResponse {

    public static final ParseField ID_FIELD = new ParseField("_id");
    public static final ParseField WATCH_FIELD = new ParseField("watch_record");

    private String recordId;
    private BytesReference contentSource;

    private Map<String, Object> data;

    public ExecuteWatchResponse() {
    }

    public ExecuteWatchResponse(String recordId, BytesReference contentSource) {
        this.recordId = recordId;
        this.contentSource = contentSource;
    }

    /**
     * @return The id of the watch record holding the watch execution result.
     */
    public String getRecordId() {
        return recordId;
    }

    /**
     * @return The watch record source
     */
    public BytesReference getRecord() {
        return contentSource;
    }

    /**
     * Returns the watch record as a map
     *
     * Use {@link org.elasticsearch.common.xcontent.ObjectPath} to navigate through the data
     */
    @SuppressWarnings("unchecked")
    public Map<String, Object> getRecordAsMap() {
        if (data == null) {
            // EMPTY is safe here because we never use namedObject
            try (InputStream stream = contentSource.streamInput();
                 XContentParser parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, null, stream)) {
                data = (Map<String, Object>) XContentUtils.readValue(parser, parser.nextToken());
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to read value", ex);
            }
        }
        return data;
    }

    private static final ConstructingObjectParser<ExecuteWatchResponse, Void> PARSER
        = new ConstructingObjectParser<>("x_pack_execute_watch_response", true,
        (fields) -> new ExecuteWatchResponse((String)fields[0], (BytesReference) fields[1]));
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> readBytesReference(p), WATCH_FIELD);
    }

    public static ExecuteWatchResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private static BytesReference readBytesReference(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder);
        }
    }
}
