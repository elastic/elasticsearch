/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
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
