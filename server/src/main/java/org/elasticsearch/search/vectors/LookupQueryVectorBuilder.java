/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.search.fetch.StoredFieldsContext._NONE_;

/*
* A QueryVectorBuilder that looks up a vector from a document stored in an index
*/
public class LookupQueryVectorBuilder implements QueryVectorBuilder {
    public static final TransportVersion LOOKUP_QVB_TV = TransportVersion.fromName("lookup_query_vector_builder");

    private static final String NAME_STR = "lookup";
    public static final ParseField NAME = new ParseField(NAME_STR);

    public static final ParseField ID_FIELD = new ParseField("id");
    public static final ParseField INDEX_FIELD = new ParseField("index");
    public static final ParseField PATH_FIELD = new ParseField("path");
    public static final ParseField ROUTING_FIELD = new ParseField("routing");

    private static final ConstructingObjectParser<LookupQueryVectorBuilder, Void> PARSER = new ConstructingObjectParser<>(
        NAME_STR,
        args -> new LookupQueryVectorBuilder((String) args[0], (String) args[1], (String) args[2], (String) args[3])
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ID_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PATH_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ROUTING_FIELD);
    }

    public static LookupQueryVectorBuilder fromXContent(org.elasticsearch.xcontent.XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final String id;
    private final String index;
    private final String path;
    private final String routing;

    /**
     * Create a new LookupQueryVectorBuilder
     * @param id    the document id to lookup
     * @param index the index where the document is stored
     * @param path  the path to the vector field in the document
     * @param routing the routing value to use when looking up the document (can be null)
     */
    public LookupQueryVectorBuilder(String id, String index, String path, String routing) {
        this.id = id;
        this.index = index;
        this.path = path;
        this.routing = routing;
    }

    public LookupQueryVectorBuilder(StreamInput in) throws IOException {
        this.id = in.readString();
        this.index = in.readString();
        this.path = in.readString();
        this.routing = in.readOptionalString();
    }

    public String getId() {
        return id;
    }

    public String getIndex() {
        return index;
    }

    public String getPath() {
        return path;
    }

    public String getRouting() {
        return routing;
    }

    @Override
    public void buildVector(Client client, ActionListener<float[]> listener) {
        client.prepareSearch(index)
            .setQuery(new IdsQueryBuilder().addIds(id))
            .setRouting(routing)
            .setPreference("_local")
            .setFetchSource(false)
            .storedFields(_NONE_)
            .addDocValueField(path)
            .setSize(1)
            .execute(ActionListener.wrap(searchResponse -> {
                if (searchResponse.getHits().getHits().length == 0) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Document with id [" + id + "] not found in index [" + index + "] for lookup query vector builder.",
                            RestStatus.NOT_FOUND
                        )
                    );
                    return;
                }
                var fieldsMap = searchResponse.getHits().getHits()[0].getFields();
                if (fieldsMap.get(path) == null || fieldsMap.get(path).getValue() == null) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Field [" + path + "] not found in document with id [" + id + "] for lookup query vector builder.",
                            RestStatus.NOT_FOUND
                        )
                    );
                    return;
                }
                Object obj = fieldsMap.get(path).getValue();
                // convert byte[] to float[] if needed
                final float[] value;
                switch (obj) {
                    case byte[] bytes -> {
                        value = new float[bytes.length];
                        for (int i = 0; i < bytes.length; i++) {
                            value[i] = bytes[i];
                        }
                    }
                    case Byte[] bytes -> {
                        value = new float[bytes.length];
                        for (int i = 0; i < bytes.length; i++) {
                            value[i] = bytes[i];
                        }
                    }
                    case float[] floats -> value = floats;
                    case Float[] floats -> {
                        value = new float[floats.length];
                        for (int i = 0; i < floats.length; i++) {
                            value[i] = floats[i];
                        }
                    }
                    case Object[] arr -> {
                        value = new float[arr.length];
                        for (int i = 0; i < arr.length; i++) {
                            if (arr[i] instanceof Number num) {
                                value[i] = num.floatValue();
                            } else {
                                listener.onFailure(
                                    new IllegalArgumentException(
                                        "Element at index ["
                                            + i
                                            + "] in field ["
                                            + path
                                            + "] in document with id ["
                                            + id
                                            + "] is of unsupported type ["
                                            + arr[i].getClass().getName()
                                            + "] for lookup query vector builder."
                                    )
                                );
                                return;
                            }
                        }
                    }
                    default -> {
                        listener.onFailure(
                            new IllegalArgumentException(
                                "Field ["
                                    + path
                                    + "] in document with id ["
                                    + id
                                    + "] is of unsupported type ["
                                    + obj.getClass().getName()
                                    + "] for lookup query vector builder."
                            )
                        );
                        return;
                    }
                }
                listener.onResponse(value);
            }, listener::onFailure));
    }

    @Override
    public String getWriteableName() {
        return NAME_STR;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return LOOKUP_QVB_TV;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeString(index);
        out.writeString(path);
        out.writeOptionalString(routing);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID_FIELD.getPreferredName(), id);
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.field(PATH_FIELD.getPreferredName(), path);
        if (routing != null) {
            builder.field(ROUTING_FIELD.getPreferredName(), routing);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        LookupQueryVectorBuilder that = (LookupQueryVectorBuilder) o;
        return Objects.equals(id, that.id)
            && Objects.equals(index, that.index)
            && Objects.equals(path, that.path)
            && Objects.equals(routing, that.routing);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, index, path, routing);
    }
}
