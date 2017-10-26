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

package org.elasticsearch.index.get;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public class GetResult implements Streamable, Iterable<DocumentField>, ToXContentObject {

    public static final String _INDEX = "_index";
    public static final String _TYPE = "_type";
    public static final String _ID = "_id";
    private static final String _VERSION = "_version";
    private static final String FOUND = "found";
    private static final String FIELDS = "fields";

    private String index;
    private String type;
    private String id;
    private long version;
    private boolean exists;
    private Map<String, DocumentField> fields;
    private Map<String, Object> sourceAsMap;
    private BytesReference source;
    private byte[] sourceAsBytes;

    GetResult() {
    }

    public GetResult(String index, String type, String id, long version, boolean exists, BytesReference source,
                     Map<String, DocumentField> fields) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.version = version;
        this.exists = exists;
        this.source = source;
        this.fields = fields;
        if (this.fields == null) {
            this.fields = emptyMap();
        }
    }

    /**
     * Does the document exist.
     */
    public boolean isExists() {
        return exists;
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return index;
    }

    /**
     * The type of the document.
     */
    public String getType() {
        return type;
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return id;
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return version;
    }

    /**
     * The source of the document if exists.
     */
    public byte[] source() {
        if (source == null) {
            return null;
        }
        if (sourceAsBytes != null) {
            return sourceAsBytes;
        }
        this.sourceAsBytes = BytesReference.toBytes(sourceRef());
        return this.sourceAsBytes;
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference sourceRef() {
        if (source == null) {
            return null;
        }

        try {
            this.source = CompressorFactory.uncompressIfNeeded(this.source);
            return this.source;
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to decompress source", e);
        }
    }

    /**
     * Internal source representation, might be compressed....
     */
    public BytesReference internalSourceRef() {
        return source;
    }

    /**
     * Is the source empty (not available) or not.
     */
    public boolean isSourceEmpty() {
        return source == null;
    }

    /**
     * The source of the document (as a string).
     */
    public String sourceAsString() {
        if (source == null) {
            return null;
        }
        BytesReference source = sourceRef();
        try {
            return XContentHelper.convertToJson(source, false);
        } catch (IOException e) {
            throw new ElasticsearchParseException("failed to convert source to a json string");
        }
    }

    /**
     * The source of the document (As a map).
     */
    @SuppressWarnings({"unchecked"})
    public Map<String, Object> sourceAsMap() throws ElasticsearchParseException {
        if (source == null) {
            return null;
        }
        if (sourceAsMap != null) {
            return sourceAsMap;
        }

        sourceAsMap = SourceLookup.sourceAsMap(source);
        return sourceAsMap;
    }

    public Map<String, Object> getSource() {
        return sourceAsMap();
    }

    public Map<String, DocumentField> getFields() {
        return fields;
    }

    public DocumentField field(String name) {
        return fields.get(name);
    }

    @Override
    public Iterator<DocumentField> iterator() {
        if (fields == null) {
            return Collections.emptyIterator();
        }
        return fields.values().iterator();
    }

    public XContentBuilder toXContentEmbedded(XContentBuilder builder, Params params) throws IOException {
        List<DocumentField> metaFields = new ArrayList<>();
        List<DocumentField> otherFields = new ArrayList<>();
        if (fields != null && !fields.isEmpty()) {
            for (DocumentField field : fields.values()) {
                if (field.getValues().isEmpty()) {
                    continue;
                }
                if (field.isMetadataField()) {
                    metaFields.add(field);
                } else {
                    otherFields.add(field);
                }
            }
        }

        for (DocumentField field : metaFields) {
            Object value = field.getValue();
            builder.field(field.getName(), value);
        }

        builder.field(FOUND, exists);

        if (source != null) {
            XContentHelper.writeRawField(SourceFieldMapper.NAME, source, builder, params);
        }

        if (!otherFields.isEmpty()) {
            builder.startObject(FIELDS);
            for (DocumentField field : otherFields) {
                field.toXContent(builder, params);
            }
            builder.endObject();
        }
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(_INDEX, index);
        builder.field(_TYPE, type);
        builder.field(_ID, id);
        if (isExists()) {
            if (version != -1) {
                builder.field(_VERSION, version);
            }
            toXContentEmbedded(builder, params);
        } else {
            builder.field(FOUND, false);
        }
        builder.endObject();
        return builder;
    }

    public static GetResult fromXContentEmbedded(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);

        String currentFieldName = parser.currentName();
        String index = null, type = null, id = null;
        long version = -1;
        Boolean found = null;
        BytesReference source = null;
        Map<String, DocumentField> fields = new HashMap<>();
        while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (_INDEX.equals(currentFieldName)) {
                    index = parser.text();
                } else if (_TYPE.equals(currentFieldName)) {
                    type = parser.text();
                } else if (_ID.equals(currentFieldName)) {
                    id = parser.text();
                }  else if (_VERSION.equals(currentFieldName)) {
                    version = parser.longValue();
                } else if (FOUND.equals(currentFieldName)) {
                    found = parser.booleanValue();
                } else {
                    fields.put(currentFieldName, new DocumentField(currentFieldName, Collections.singletonList(parser.objectText())));
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (SourceFieldMapper.NAME.equals(currentFieldName)) {
                    try (XContentBuilder builder = XContentBuilder.builder(parser.contentType().xContent())) {
                        //the original document gets slightly modified: whitespaces or pretty printing are not preserved,
                        //it all depends on the current builder settings
                        builder.copyCurrentStructure(parser);
                        source = builder.bytes();
                    }
                } else if (FIELDS.equals(currentFieldName)) {
                    while(parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        DocumentField getField = DocumentField.fromXContent(parser);
                        fields.put(getField.getName(), getField);
                    }
                } else {
                    parser.skipChildren(); // skip potential inner objects for forward compatibility
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren(); // skip potential inner arrays for forward compatibility
            }
        }
        return new GetResult(index, type, id, version, found, source, fields);
    }

    public static GetResult fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);

        return fromXContentEmbedded(parser);
    }

    public static GetResult readGetResult(StreamInput in) throws IOException {
        GetResult result = new GetResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        type = in.readOptionalString();
        id = in.readString();
        version = in.readLong();
        exists = in.readBoolean();
        if (exists) {
            source = in.readBytesReference();
            if (source.length() == 0) {
                source = null;
            }
            int size = in.readVInt();
            if (size == 0) {
                fields = emptyMap();
            } else {
                fields = new HashMap<>(size);
                for (int i = 0; i < size; i++) {
                    DocumentField field = DocumentField.readDocumentField(in);
                    fields.put(field.getName(), field);
                }
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeOptionalString(type);
        out.writeString(id);
        out.writeLong(version);
        out.writeBoolean(exists);
        if (exists) {
            out.writeBytesReference(source);
            if (fields == null) {
                out.writeVInt(0);
            } else {
                out.writeVInt(fields.size());
                for (DocumentField field : fields.values()) {
                    field.writeTo(out);
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetResult getResult = (GetResult) o;
        return version == getResult.version &&
                exists == getResult.exists &&
                Objects.equals(index, getResult.index) &&
                Objects.equals(type, getResult.type) &&
                Objects.equals(id, getResult.id) &&
                Objects.equals(fields, getResult.fields) &&
                Objects.equals(sourceAsMap(), getResult.sourceAsMap());
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, type, id, version, exists, fields, sourceAsMap());
    }
}

