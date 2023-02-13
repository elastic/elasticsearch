/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.rest.action.document.RestMultiGetAction;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiGetResponse extends ActionResponse implements Iterable<MultiGetItemResponse>, ToXContentObject {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MultiGetResponse.class);

    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField TYPE = new ParseField("_type");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ERROR = new ParseField("error");
    private static final ParseField DOCS = new ParseField("docs");

    /**
     * Represents a failure.
     */
    public static class Failure implements Writeable, ToXContentObject {

        private final String index;
        private final String id;
        private final Exception exception;

        public Failure(String index, String id, Exception exception) {
            this.index = index;
            this.id = id;
            this.exception = exception;
        }

        Failure(StreamInput in) throws IOException {
            index = in.readString();
            if (in.getTransportVersion().before(TransportVersion.V_8_0_0)) {
                in.readOptionalString();
            }
            id = in.readString();
            exception = in.readException();
        }

        /**
         * The index name of the action.
         */
        public String getIndex() {
            return this.index;
        }

        /**
         * The id of the action.
         */
        public String getId() {
            return id;
        }

        /**
         * The failure message.
         */
        public String getMessage() {
            return exception != null ? exception.getMessage() : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            if (out.getTransportVersion().before(TransportVersion.V_8_0_0)) {
                out.writeOptionalString(MapperService.SINGLE_MAPPING_NAME);
            }
            out.writeString(id);
            out.writeException(exception);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX.getPreferredName(), index);
            if (builder.getRestApiVersion() == RestApiVersion.V_7) {
                builder.field(MapperService.TYPE_FIELD_NAME, MapperService.SINGLE_MAPPING_NAME);
            }
            builder.field(ID.getPreferredName(), id);
            ElasticsearchException.generateFailureXContent(builder, params, exception, true);
            builder.endObject();
            return builder;
        }

        public Exception getFailure() {
            return exception;
        }
    }

    private final MultiGetItemResponse[] responses;

    public MultiGetResponse(MultiGetItemResponse[] responses) {
        this.responses = responses;
    }

    MultiGetResponse(StreamInput in) throws IOException {
        super(in);
        responses = in.readArray(MultiGetItemResponse::new, MultiGetItemResponse[]::new);
    }

    public MultiGetItemResponse[] getResponses() {
        return this.responses;
    }

    @Override
    public Iterator<MultiGetItemResponse> iterator() {
        return Iterators.forArray(responses);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray(DOCS.getPreferredName());
        for (MultiGetItemResponse response : responses) {
            if (response.isFailed()) {
                Failure failure = response.getFailure();
                failure.toXContent(builder, params);
            } else {
                GetResponse getResponse = response.getResponse();
                getResponse.toXContent(builder, params);
            }
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static MultiGetResponse fromXContent(XContentParser parser) throws IOException {
        String currentFieldName = null;
        List<MultiGetItemResponse> items = new ArrayList<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    break;
                case START_ARRAY:
                    if (DOCS.getPreferredName().equals(currentFieldName)) {
                        for (token = parser.nextToken(); token != Token.END_ARRAY; token = parser.nextToken()) {
                            if (token == Token.START_OBJECT) {
                                items.add(parseItem(parser));
                            }
                        }
                    }
                    break;
                default:
                    // If unknown tokens are encounter then these should be ignored, because
                    // this is parsing logic on the client side.
                    break;
            }
        }
        return new MultiGetResponse(items.toArray(new MultiGetItemResponse[0]));
    }

    private static MultiGetItemResponse parseItem(XContentParser parser) throws IOException {
        String currentFieldName = null;
        String index = null;
        String id = null;
        ElasticsearchException exception = null;
        GetResult getResult = null;
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    currentFieldName = parser.currentName();
                    if (INDEX.match(currentFieldName, parser.getDeprecationHandler()) == false
                        && ID.match(currentFieldName, parser.getDeprecationHandler()) == false
                        && ERROR.match(currentFieldName, parser.getDeprecationHandler()) == false) {
                        getResult = GetResult.fromXContentEmbedded(parser, index, id);
                    }
                    break;
                case VALUE_STRING:
                    if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                        index = parser.text();
                    } else if (TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                        deprecationLogger.compatibleCritical("mget_with_types", RestMultiGetAction.TYPES_DEPRECATION_MESSAGE);
                    } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                        id = parser.text();
                    }
                    break;
                case START_OBJECT:
                    if (ERROR.match(currentFieldName, parser.getDeprecationHandler())) {
                        exception = ElasticsearchException.fromXContent(parser);
                    }
                    break;
                default:
                    // If unknown tokens are encounter then these should be ignored, because
                    // this is parsing logic on the client side.
                    break;
            }
            if (getResult != null) {
                break;
            }
        }

        if (exception != null) {
            return new MultiGetItemResponse(null, new Failure(index, id, exception));
        } else {
            GetResponse getResponse = new GetResponse(getResult);
            return new MultiGetItemResponse(getResponse, null);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(responses);
    }
}
