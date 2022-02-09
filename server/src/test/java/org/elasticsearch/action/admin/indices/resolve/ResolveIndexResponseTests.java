/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.resolve;

import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedAlias;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex;
import org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream.BACKING_INDICES_FIELD;
import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedDataStream.TIMESTAMP_FIELD;
import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex.ALIASES_FIELD;
import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex.ATTRIBUTES_FIELD;
import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndex.DATA_STREAM_FIELD;
import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.ResolvedIndexAbstraction.NAME_FIELD;
import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.Response.DATA_STREAMS_FIELD;
import static org.elasticsearch.action.admin.indices.resolve.ResolveIndexAction.Response.INDICES_FIELD;

public class ResolveIndexResponseTests extends AbstractSerializingTestCase<Response> {

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) throws IOException {
        return responseFromXContent(parser);
    }

    @Override
    protected Response createTestInstance() {
        final List<ResolvedIndex> indices = new ArrayList<>();
        final List<ResolvedAlias> aliases = new ArrayList<>();
        final List<ResolvedDataStream> dataStreams = new ArrayList<>();

        int num = randomIntBetween(0, 8);
        for (int k = 0; k < num; k++) {
            indices.add(createTestResolvedIndexInstance());
        }
        num = randomIntBetween(0, 8);
        for (int k = 0; k < num; k++) {
            aliases.add(createTestResolvedAliasInstance());
        }
        num = randomIntBetween(0, 8);
        for (int k = 0; k < num; k++) {
            dataStreams.add(createTestResolvedDataStreamInstance());
        }

        return new Response(indices, aliases, dataStreams);
    }

    private static ResolvedIndex createTestResolvedIndexInstance() {
        String name = randomAlphaOfLength(6);
        String[] aliases = randomStringArray(0, 5);
        String[] attributes = randomSubsetOf(List.of("open", "hidden", "frozen")).toArray(Strings.EMPTY_ARRAY);
        String dataStream = randomBoolean() ? randomAlphaOfLength(6) : null;

        return new ResolvedIndex(name, aliases, attributes, dataStream);
    }

    private static ResolvedAlias createTestResolvedAliasInstance() {
        String name = randomAlphaOfLength(6);
        String[] indices = randomStringArray(1, 6);
        return new ResolvedAlias(name, indices);
    }

    private static ResolvedDataStream createTestResolvedDataStreamInstance() {
        String name = randomAlphaOfLength(6);
        String[] backingIndices = randomStringArray(1, 6);
        String timestampField = randomAlphaOfLength(6);
        return new ResolvedDataStream(name, backingIndices, timestampField);
    }

    static String[] randomStringArray(int minLength, int maxLength) {
        int num = randomIntBetween(minLength, maxLength);
        String[] stringArray = new String[num];
        for (int k = 0; k < num; k++) {
            stringArray[k] = randomAlphaOfLength(6);
        }
        return stringArray;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ResolvedIndex, Void> INDEX_PARSER = new ConstructingObjectParser<>(
        "resolved_index",
        args -> new ResolvedIndex(
            (String) args[0],
            args[1] != null ? ((List<String>) args[1]).toArray(Strings.EMPTY_ARRAY) : new String[0],
            ((List<String>) args[2]).toArray(Strings.EMPTY_ARRAY),
            (String) args[3]
        )
    );
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Response, Void> RESPONSE_PARSER = new ConstructingObjectParser<>(
        "resolve_index_response",
        args -> new Response((List<ResolvedIndex>) args[0], (List<ResolvedAlias>) args[1], (List<ResolvedDataStream>) args[2])
    );
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ResolvedAlias, Void> ALIAS_PARSER = new ConstructingObjectParser<>(
        "resolved_alias",
        args -> new ResolvedAlias((String) args[0], ((List<String>) args[1]).toArray(Strings.EMPTY_ARRAY))
    );
    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<ResolvedDataStream, Void> DATA_STREAM_PARSER = new ConstructingObjectParser<>(
        "resolved_data_stream",
        args -> new ResolvedDataStream((String) args[0], ((List<String>) args[1]).toArray(Strings.EMPTY_ARRAY), (String) args[2])
    );

    static {
        INDEX_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        INDEX_PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), ALIASES_FIELD);
        INDEX_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), ATTRIBUTES_FIELD);
        INDEX_PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), DATA_STREAM_FIELD);
        ALIAS_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        ALIAS_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES_FIELD);
        RESPONSE_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> indexFromXContent(p), INDICES_FIELD);
        RESPONSE_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> aliasFromXContent(p), ALIASES_FIELD);
        RESPONSE_PARSER.declareObjectArray(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> dataStreamFromXContent(p),
            DATA_STREAMS_FIELD
        );
        DATA_STREAM_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        DATA_STREAM_PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), BACKING_INDICES_FIELD);
        DATA_STREAM_PARSER.declareString(ConstructingObjectParser.constructorArg(), TIMESTAMP_FIELD);
    }

    static ResolvedIndex indexFromXContent(XContentParser parser) throws IOException {
        return INDEX_PARSER.parse(parser, null);
    }

    public static Response responseFromXContent(XContentParser parser) throws IOException {
        return RESPONSE_PARSER.parse(parser, null);
    }

    public static ResolvedAlias aliasFromXContent(XContentParser parser) throws IOException {
        return ALIAS_PARSER.parse(parser, null);
    }

    public static ResolvedDataStream dataStreamFromXContent(XContentParser parser) throws IOException {
        return DATA_STREAM_PARSER.parse(parser, null);
    }
}
