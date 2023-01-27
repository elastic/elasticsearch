/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IndexedQueryVectorBuilder implements QueryVectorBuilder {

    public static final String NAME = "indexed_query_vector";

    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField ID = new ParseField("id");
    private static final ParseField PATH = new ParseField("path");
    private static final ParseField ROUTING = new ParseField("routing");

    private static final ConstructingObjectParser<IndexedQueryVectorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        "indexed_query_vector_parser",
        false,
        a -> new IndexedQueryVectorBuilder((String) a[0], (String) a[1], (String) a[2], (String) a[3])
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PATH);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ROUTING);
    }

    public static IndexedQueryVectorBuilder fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final String index;
    private final String id;
    private final String path;
    private final String routing;

    public IndexedQueryVectorBuilder(String index, String id, String path, String routing) {
        this.index = index;
        this.id = id;
        this.path = path;
        this.routing = routing;
    }

    public IndexedQueryVectorBuilder(StreamInput input) throws IOException {
        this.index = input.readString();
        this.id = input.readString();
        this.path = input.readString();
        this.routing = input.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        builder.field(ID.getPreferredName(), id);
        builder.field(PATH.getPreferredName(), path);
        if (routing != null) {
            builder.field(ROUTING.getPreferredName(), routing);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_7_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(id);
        out.writeString(path);
        out.writeOptionalString(routing);
    }

    @Override
    public void buildVector(Client client, ActionListener<float[]> listener) {
        GetRequest getRequest = new GetRequest(index, id);
        if (routing != null) {
            getRequest.routing(routing);
        }
        getRequest.preference("_local");
        client.get(getRequest, listener.delegateFailure((l, response) -> {
            try {
                if (response.isExists() == false) {
                    throw new IllegalArgumentException("Query vector with ID [" + getRequest.id() + "] not found");
                }
                if (response.isSourceEmpty()) {
                    throw new IllegalArgumentException("Query vector with ID [" + getRequest.id() + "] source disabled");
                }

                String[] pathElements = path.split("\\.");
                int currentPathSlot = 0;

                // It is safe to use EMPTY here because this never uses namedObject
                try (
                    XContentParser parser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        response.getSourceAsBytesRef()
                    )
                ) {
                    XContentParser.Token currentToken;
                    while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (currentToken == XContentParser.Token.FIELD_NAME) {
                            if (pathElements[currentPathSlot].equals(parser.currentName())) {
                                parser.nextToken();
                                if (++currentPathSlot == pathElements.length) {
                                    List<Float> floatList = new ArrayList<>();
                                    if (parser.currentToken() != XContentParser.Token.START_ARRAY) {
                                        throw new XContentParseException(
                                            parser.getTokenLocation(),
                                            "[float_array] failed to parse, expected array but found [" + parser.currentToken() + "]"
                                        );
                                    }
                                    for (currentToken = parser.nextToken(); currentToken != XContentParser.Token.END_ARRAY; currentToken =
                                        parser.nextToken()) {
                                        floatList.add(parser.floatValue(true));
                                    }
                                    final float[] floats = new float[floatList.size()];
                                    int i = 0;
                                    for (Float f : floatList) {
                                        floats[i++] = f;
                                    }
                                    l.onResponse(floats);
                                }
                            } else {
                                parser.nextToken();
                                parser.skipChildren();
                            }
                        }
                    }
                    throw new IllegalStateException(
                        "Query vector with name [" + getRequest.id() + "] found but missing " + path + " field"
                    );
                }
            } catch (Exception e) {
                l.onFailure(e);
            }
        }));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexedQueryVectorBuilder that = (IndexedQueryVectorBuilder) o;
        return Objects.equals(index, that.index)
            && Objects.equals(id, that.id)
            && Objects.equals(path, that.path)
            && Objects.equals(routing, that.routing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, id, path, routing);
    }

    public String getIndex() {
        return index;
    }

    public String getId() {
        return id;
    }

    public String getPath() {
        return path;
    }

    public String getRouting() {
        return routing;
    }
}
