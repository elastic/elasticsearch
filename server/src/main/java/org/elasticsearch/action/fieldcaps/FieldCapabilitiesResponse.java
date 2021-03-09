/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Response for {@link FieldCapabilitiesRequest} requests.
 */
public class FieldCapabilitiesResponse extends ActionResponse implements StatusToXContentObject {
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField FIELDS_FIELD = new ParseField("fields");
    private static final ParseField FAILED_INDICES_FIELD = new ParseField("failed_indices");
    private static final ParseField FAILURES_FIELD = new ParseField("failures");

    private final String[] indices;
    private final Map<String, Map<String, FieldCapabilities>> responseMap;
    private final Map<String, Exception> failureMap;
    private final List<FieldCapabilitiesIndexResponse> indexResponses;

    public FieldCapabilitiesResponse(
        String[] indices,
        Map<String, Map<String, FieldCapabilities>> responseMap,
        Map<String, Exception> failureMap
    ) {
        this(indices, responseMap, Collections.emptyList(), failureMap);
    }

    public FieldCapabilitiesResponse(String[] indices, Map<String, Map<String, FieldCapabilities>> responseMap) {
        this(indices, responseMap, Collections.emptyList(), Collections.emptyMap());
    }

    FieldCapabilitiesResponse(List<FieldCapabilitiesIndexResponse> indexResponses) {
        this(Strings.EMPTY_ARRAY, Collections.emptyMap(), indexResponses, Collections.emptyMap());
    }

    private FieldCapabilitiesResponse(String[] indices, Map<String, Map<String, FieldCapabilities>> responseMap,
                                      List<FieldCapabilitiesIndexResponse> indexResponses, Map<String, Exception> failureMap) {
        this.responseMap = Objects.requireNonNull(responseMap);
        this.indexResponses = Objects.requireNonNull(indexResponses);
        this.indices = indices;
        this.failureMap = failureMap;
    }

    public FieldCapabilitiesResponse(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
            indices = in.readStringArray();
        } else {
            indices = Strings.EMPTY_ARRAY;
        }
        this.responseMap = in.readMap(StreamInput::readString, FieldCapabilitiesResponse::readField);
        indexResponses = in.readList(FieldCapabilitiesIndexResponse::new);
        if (in.getVersion().onOrAfter(Version.CURRENT)) {
            int failuresSize = in.readVInt();
            failureMap = new HashMap<>(failuresSize);
            for (int i = 0; i < failuresSize; i++) {
                String index = in.readString();
                failureMap.put(index, in.readException());
            }
        } else {
            this.failureMap = Collections.emptyMap();
        }
    }

    /**
     * Used for serialization
     */
    FieldCapabilitiesResponse() {
        this(Strings.EMPTY_ARRAY, Collections.emptyMap(), Collections.emptyList(), Collections.emptyMap());
    }

    /**
     * Get the concrete list of indices that were requested and returned a response.
     */
    public String[] getIndices() {
        return indices;
    }

    /**
     * Get the concrete list of indices that failed
     */
    public String[] getFailedIndices() {
        return this.failureMap.keySet().toArray(size -> new String[size]);
    }

    /**
     * Get the field capabilities map.
     */
    public Map<String, Map<String, FieldCapabilities>> get() {
        return responseMap;
    }

    /**
     * Get possible request failures keyed by index name
     */
    public Map<String, Exception> getFailures() {
        return failureMap;
    }

    /**
     * Returns the actual per-index field caps responses
     */
    List<FieldCapabilitiesIndexResponse> getIndexResponses() {
        return indexResponses;
    }

    /**
     *
     * Get the field capabilities per type for the provided {@code field}.
     */
    public Map<String, FieldCapabilities> getField(String field) {
        return responseMap.get(field);
    }

    private static Map<String, FieldCapabilities> readField(StreamInput in) throws IOException {
        return in.readMap(StreamInput::readString, FieldCapabilities::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
            out.writeStringArray(indices);
        }
        out.writeMap(responseMap, StreamOutput::writeString, FieldCapabilitiesResponse::writeField);
        out.writeList(indexResponses);
        if (out.getVersion().onOrAfter(Version.CURRENT)) {
            out.writeVInt(failureMap.size());
            for (String queryId : failureMap.keySet()) {
                out.writeString(queryId);
                out.writeException(failureMap.get(queryId));
            }
        }
    }

    private static void writeField(StreamOutput out, Map<String, FieldCapabilities> map) throws IOException {
        out.writeMap(map, StreamOutput::writeString, (valueOut, fc) -> fc.writeTo(valueOut));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (indexResponses.size() > 0) {
            throw new IllegalStateException("cannot serialize non-merged response");
        }
        builder.startObject();
        builder.field(INDICES_FIELD.getPreferredName(), indices);
        builder.field(FIELDS_FIELD.getPreferredName(), responseMap);
        if (this.failureMap.size() > 0) {
            builder.field(FAILED_INDICES_FIELD.getPreferredName(), failureMap.size());
            builder.startObject(FAILURES_FIELD.getPreferredName());
            for (String key : failureMap.keySet()) {
                builder.startObject(key);
                ElasticsearchException.generateFailureXContent(builder, params, failureMap.get(key), true);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static FieldCapabilitiesResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilitiesResponse, Void> PARSER =
        new ConstructingObjectParser<>("field_capabilities_response", true, a -> {
            Map<String, Map<String, FieldCapabilities>> responseMap = ((List<Tuple<String, Map<String, FieldCapabilities>>>) a[0]).stream()
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            List<String> indices = a[1] == null ? Collections.emptyList() : (List<String>) a[1];
            Map<String, Exception> failures = a[2] == null
                ? Collections.emptyMap()
                : ((List<Tuple<String, Exception>>) a[2]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2));
            return new FieldCapabilitiesResponse(indices.stream().toArray(String[]::new), responseMap, failures);
        });

    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> {
            Map<String, FieldCapabilities> typeToCapabilities = parseTypeToCapabilities(p, n);
            return new Tuple<>(n, typeToCapabilities);
        }, FIELDS_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INDICES_FIELD);
        PARSER.declareNamedObjects(ConstructingObjectParser.optionalConstructorArg(), (p, c, n) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, p.nextToken(), p);
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, p.nextToken(), p);
            Tuple<String, ElasticsearchException> tuple = new Tuple<>(n, ElasticsearchException.failureFromXContent(p));
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, p.nextToken(), p);
            return tuple;
        }, FAILURES_FIELD);
    }

    private static Map<String, FieldCapabilities> parseTypeToCapabilities(XContentParser parser, String name) throws IOException {
        Map<String, FieldCapabilities> typeToCapabilities = new HashMap<>();

        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
            String type = parser.currentName();
            FieldCapabilities capabilities = FieldCapabilities.fromXContent(name, parser);
            typeToCapabilities.put(type, capabilities);
        }
        return typeToCapabilities;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilitiesResponse that = (FieldCapabilitiesResponse) o;
        return Arrays.equals(indices, that.indices) &&
            Objects.equals(responseMap, that.responseMap) &&
            Objects.equals(indexResponses, that.indexResponses) &&
            Objects.equals(failureMap, that.failureMap);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(responseMap, indexResponses, failureMap);
        result = 31 * result + Arrays.hashCode(indices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public RestStatus status() {
        RestStatus status = RestStatus.OK;
        if (indices.length == 0 && failureMap.size() > 0) {
            for (Exception failure : failureMap.values()) {
                RestStatus failureStatus = ExceptionsHelper.status(failure);
                if (failureStatus.getStatus() >= status.getStatus()) {
                    status = failureStatus;
                }
            }
            return status;
        }
        return status;
    }
}
