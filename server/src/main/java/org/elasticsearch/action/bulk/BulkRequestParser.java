/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentEOFException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

/**
 * Helper to parse bulk requests. This should be considered an internal class.
 */
public final class BulkRequestParser {
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(BulkRequestParser.class);
    private static final Set<String> SUPPORTED_ACTIONS = Set.of("create", "index", "update", "delete");
    private static final String STRICT_ACTION_PARSING_WARNING_KEY = "bulk_request_strict_action_parsing";

    private static final ParseField INDEX = new ParseField("_index");
    private static final ParseField TYPE = new ParseField("_type");
    private static final ParseField ID = new ParseField("_id");
    private static final ParseField ROUTING = new ParseField("routing");
    private static final ParseField OP_TYPE = new ParseField("op_type");
    private static final ParseField VERSION = new ParseField("version");
    private static final ParseField VERSION_TYPE = new ParseField("version_type");
    private static final ParseField RETRY_ON_CONFLICT = new ParseField("retry_on_conflict");
    private static final ParseField PIPELINE = new ParseField("pipeline");
    private static final ParseField SOURCE = new ParseField("_source");
    private static final ParseField IF_SEQ_NO = new ParseField("if_seq_no");
    private static final ParseField IF_PRIMARY_TERM = new ParseField("if_primary_term");
    private static final ParseField REQUIRE_ALIAS = new ParseField(DocWriteRequest.REQUIRE_ALIAS);
    private static final ParseField DYNAMIC_TEMPLATES = new ParseField("dynamic_templates");

    // TODO: Remove this parameter once the BulkMonitoring endpoint has been removed
    // for CompatibleApi V7 this means to deprecate on type, for V8+ it means to throw an error
    private final boolean deprecateOrErrorOnType;
    /**
     * Configuration for {@link XContentParser}.
     */
    private final XContentParserConfiguration config;

    /**
     * Create a new parser.
     *
     * @param deprecateOrErrorOnType whether to allow _type information in the index line; used by BulkMonitoring
     * @param restApiVersion
     */
    public BulkRequestParser(boolean deprecateOrErrorOnType, RestApiVersion restApiVersion) {
        this.deprecateOrErrorOnType = deprecateOrErrorOnType;
        this.config = XContentParserConfiguration.EMPTY.withDeprecationHandler(LoggingDeprecationHandler.INSTANCE)
            .withRestApiVersion(restApiVersion);
    }

    private static int findNextMarker(byte marker, int from, BytesReference data) {
        final int res = data.indexOf(marker, from);
        if (res != -1) {
            assert res >= 0;
            return res;
        }
        if (from != data.length()) {
            throw new IllegalArgumentException("The bulk request must be terminated by a newline [\\n]");
        }
        return res;
    }

    /**
     * Returns the sliced {@link BytesReference}. If the {@link XContentType} is JSON, the byte preceding the marker is checked to see
     * if it is a carriage return and if so, the BytesReference is sliced so that the carriage return is ignored
     */
    private static BytesReference sliceTrimmingCarriageReturn(
        BytesReference bytesReference,
        int from,
        int nextMarker,
        XContentType xContentType
    ) {
        final int length;
        if (XContentType.JSON == xContentType && bytesReference.get(nextMarker - 1) == (byte) '\r') {
            length = nextMarker - from - 1;
        } else {
            length = nextMarker - from;
        }
        return bytesReference.slice(from, length);
    }

    /**
     * Parse the provided {@code data} assuming the provided default values. Index requests
     * will be passed to the {@code indexRequestConsumer}, update requests to the
     * {@code updateRequestConsumer} and delete requests to the {@code deleteRequestConsumer}.
     */
    public void parse(
        BytesReference data,
        @Nullable String defaultIndex,
        @Nullable String defaultRouting,
        @Nullable FetchSourceContext defaultFetchSourceContext,
        @Nullable String defaultPipeline,
        @Nullable Boolean defaultRequireAlias,
        boolean allowExplicitIndex,
        XContentType xContentType,
        BiConsumer<IndexRequest, String> indexRequestConsumer,
        Consumer<UpdateRequest> updateRequestConsumer,
        Consumer<DeleteRequest> deleteRequestConsumer
    ) throws IOException {
        XContent xContent = xContentType.xContent();
        int line = 0;
        int from = 0;
        byte marker = xContent.streamSeparator();
        // Bulk requests can contain a lot of repeated strings for the index, pipeline and routing parameters. This map is used to
        // deduplicate duplicate strings parsed for these parameters. While it does not prevent instantiating the duplicate strings, it
        // reduces their lifetime to the lifetime of this parse call instead of the lifetime of the full bulk request.
        final Map<String, String> stringDeduplicator = new HashMap<>();
        boolean typesDeprecationLogged = false;

        while (true) {
            int nextMarker = findNextMarker(marker, from, data);
            if (nextMarker == -1) {
                break;
            }
            line++;

            // now parse the action
            try (XContentParser parser = createParser(xContent, data, from, nextMarker)) {
                // move pointers
                from = nextMarker + 1;

                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException(
                        "Malformed action/metadata line ["
                            + line
                            + "], expected "
                            + XContentParser.Token.START_OBJECT
                            + " but found ["
                            + token
                            + "]"
                    );
                }
                // Move to FIELD_NAME, that's the action
                token = parser.nextToken();
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new IllegalArgumentException(
                        "Malformed action/metadata line ["
                            + line
                            + "], expected "
                            + XContentParser.Token.FIELD_NAME
                            + " but found ["
                            + token
                            + "]"
                    );
                }
                String action = parser.currentName();
                if (SUPPORTED_ACTIONS.contains(action) == false) {
                    deprecationLogger.compatibleCritical(
                        STRICT_ACTION_PARSING_WARNING_KEY,
                        "Unsupported action: [{}]. Supported values are [create], [delete], [index], and [update]. "
                            + "Unsupported actions are currently accepted but will be rejected in a future version.",
                        action
                    );
                }

                String index = defaultIndex;
                String type = null;
                String id = null;
                String routing = defaultRouting;
                FetchSourceContext fetchSourceContext = defaultFetchSourceContext;
                String opType = null;
                long version = Versions.MATCH_ANY;
                VersionType versionType = VersionType.INTERNAL;
                long ifSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                long ifPrimaryTerm = UNASSIGNED_PRIMARY_TERM;
                int retryOnConflict = 0;
                String pipeline = defaultPipeline;
                boolean requireAlias = defaultRequireAlias != null && defaultRequireAlias;
                Map<String, String> dynamicTemplates = Map.of();

                // at this stage, next token can either be END_OBJECT (and use default index and type, with auto generated id)
                // or START_OBJECT which will have another set of parameters
                token = parser.nextToken();

                if (token == XContentParser.Token.START_OBJECT) {
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (INDEX.match(currentFieldName, parser.getDeprecationHandler())) {
                                if (allowExplicitIndex == false) {
                                    throw new IllegalArgumentException("explicit index in bulk is not allowed");
                                }
                                index = stringDeduplicator.computeIfAbsent(parser.text(), Function.identity());
                            } else if (TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                                if (parser.getRestApiVersion().matches(RestApiVersion.equalTo(RestApiVersion.V_7))) {
                                    // for bigger bulks, deprecation throttling might not be enough
                                    if (deprecateOrErrorOnType && typesDeprecationLogged == false) {
                                        deprecationLogger.compatibleCritical("bulk_with_types", RestBulkAction.TYPES_DEPRECATION_MESSAGE);
                                        typesDeprecationLogged = true;
                                    }
                                } else if (parser.getRestApiVersion().matches(RestApiVersion.onOrAfter(RestApiVersion.V_8))
                                    && deprecateOrErrorOnType) {
                                        throw new IllegalArgumentException(
                                            "Action/metadata line [" + line + "] contains an unknown parameter [" + currentFieldName + "]"
                                        );
                                    }
                                type = stringDeduplicator.computeIfAbsent(parser.text(), Function.identity());
                            } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                                id = parser.text();
                            } else if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                                routing = stringDeduplicator.computeIfAbsent(parser.text(), Function.identity());
                            } else if (OP_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                                opType = parser.text();
                            } else if (VERSION.match(currentFieldName, parser.getDeprecationHandler())) {
                                version = parser.longValue();
                            } else if (VERSION_TYPE.match(currentFieldName, parser.getDeprecationHandler())) {
                                versionType = VersionType.fromString(parser.text());
                            } else if (IF_SEQ_NO.match(currentFieldName, parser.getDeprecationHandler())) {
                                ifSeqNo = parser.longValue();
                            } else if (IF_PRIMARY_TERM.match(currentFieldName, parser.getDeprecationHandler())) {
                                ifPrimaryTerm = parser.longValue();
                            } else if (RETRY_ON_CONFLICT.match(currentFieldName, parser.getDeprecationHandler())) {
                                retryOnConflict = parser.intValue();
                            } else if (PIPELINE.match(currentFieldName, parser.getDeprecationHandler())) {
                                pipeline = stringDeduplicator.computeIfAbsent(parser.text(), Function.identity());
                            } else if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                                fetchSourceContext = FetchSourceContext.fromXContent(parser);
                            } else if (REQUIRE_ALIAS.match(currentFieldName, parser.getDeprecationHandler())) {
                                requireAlias = parser.booleanValue();
                            } else {
                                throw new IllegalArgumentException(
                                    "Action/metadata line [" + line + "] contains an unknown parameter [" + currentFieldName + "]"
                                );
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            throw new IllegalArgumentException(
                                "Malformed action/metadata line ["
                                    + line
                                    + "], expected a simple value for field ["
                                    + currentFieldName
                                    + "] but found ["
                                    + token
                                    + "]"
                            );
                        } else if (token == XContentParser.Token.START_OBJECT
                            && DYNAMIC_TEMPLATES.match(currentFieldName, parser.getDeprecationHandler())) {
                                dynamicTemplates = parser.mapStrings();
                            } else if (token == XContentParser.Token.START_OBJECT
                                && SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                                    fetchSourceContext = FetchSourceContext.fromXContent(parser);
                                } else if (token != XContentParser.Token.VALUE_NULL) {
                                    throw new IllegalArgumentException(
                                        "Malformed action/metadata line ["
                                            + line
                                            + "], expected a simple value for field ["
                                            + currentFieldName
                                            + "] but found ["
                                            + token
                                            + "]"
                                    );
                                }
                    }
                } else if (token != XContentParser.Token.END_OBJECT) {
                    throw new IllegalArgumentException(
                        "Malformed action/metadata line ["
                            + line
                            + "], expected "
                            + XContentParser.Token.START_OBJECT
                            + " or "
                            + XContentParser.Token.END_OBJECT
                            + " but found ["
                            + token
                            + "]"
                    );
                }
                checkBulkActionIsProperlyClosed(parser);

                if ("delete".equals(action)) {
                    if (dynamicTemplates.isEmpty() == false) {
                        throw new IllegalArgumentException(
                            "Delete request in line [" + line + "] does not accept " + DYNAMIC_TEMPLATES.getPreferredName()
                        );
                    }
                    deleteRequestConsumer.accept(
                        new DeleteRequest(index).id(id)
                            .routing(routing)
                            .version(version)
                            .versionType(versionType)
                            .setIfSeqNo(ifSeqNo)
                            .setIfPrimaryTerm(ifPrimaryTerm)
                    );
                } else {
                    nextMarker = findNextMarker(marker, from, data);
                    if (nextMarker == -1) {
                        break;
                    }
                    line++;

                    // we use internalAdd so we don't fork here, this allows us not to copy over the big byte array to small chunks
                    // of index request.
                    if ("index".equals(action)) {
                        if (opType == null) {
                            indexRequestConsumer.accept(
                                new IndexRequest(index).id(id)
                                    .routing(routing)
                                    .version(version)
                                    .versionType(versionType)
                                    .setPipeline(pipeline)
                                    .setIfSeqNo(ifSeqNo)
                                    .setIfPrimaryTerm(ifPrimaryTerm)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType)
                                    .setDynamicTemplates(dynamicTemplates)
                                    .setRequireAlias(requireAlias),
                                type
                            );
                        } else {
                            indexRequestConsumer.accept(
                                new IndexRequest(index).id(id)
                                    .routing(routing)
                                    .version(version)
                                    .versionType(versionType)
                                    .create("create".equals(opType))
                                    .setPipeline(pipeline)
                                    .setIfSeqNo(ifSeqNo)
                                    .setIfPrimaryTerm(ifPrimaryTerm)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType)
                                    .setRequireAlias(requireAlias),
                                type
                            );
                        }
                    } else if ("create".equals(action)) {
                        indexRequestConsumer.accept(
                            new IndexRequest(index).id(id)
                                .routing(routing)
                                .version(version)
                                .versionType(versionType)
                                .create(true)
                                .setPipeline(pipeline)
                                .setIfSeqNo(ifSeqNo)
                                .setIfPrimaryTerm(ifPrimaryTerm)
                                .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType)
                                .setDynamicTemplates(dynamicTemplates)
                                .setRequireAlias(requireAlias),
                            type
                        );
                    } else if ("update".equals(action)) {
                        if (version != Versions.MATCH_ANY || versionType != VersionType.INTERNAL) {
                            throw new IllegalArgumentException(
                                "Update requests do not support versioning. " + "Please use `if_seq_no` and `if_primary_term` instead"
                            );
                        }
                        // TODO: support dynamic_templates in update requests
                        if (dynamicTemplates.isEmpty() == false) {
                            throw new IllegalArgumentException(
                                "Update request in line [" + line + "] does not accept " + DYNAMIC_TEMPLATES.getPreferredName()
                            );
                        }
                        UpdateRequest updateRequest = new UpdateRequest().index(index)
                            .id(id)
                            .routing(routing)
                            .retryOnConflict(retryOnConflict)
                            .setIfSeqNo(ifSeqNo)
                            .setIfPrimaryTerm(ifPrimaryTerm)
                            .setRequireAlias(requireAlias)
                            .routing(routing);
                        try (
                            XContentParser sliceParser = createParser(
                                xContent,
                                sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType)
                            )
                        ) {
                            updateRequest.fromXContent(sliceParser);
                        }
                        if (fetchSourceContext != null) {
                            updateRequest.fetchSource(fetchSourceContext);
                        }
                        IndexRequest upsertRequest = updateRequest.upsertRequest();
                        if (upsertRequest != null) {
                            upsertRequest.setPipeline(defaultPipeline);
                        }

                        updateRequestConsumer.accept(updateRequest);
                    }
                    // move pointers
                    from = nextMarker + 1;
                }
            }
        }
    }

    private static void checkBulkActionIsProperlyClosed(XContentParser parser) throws IOException {
        XContentParser.Token token;
        try {
            token = parser.nextToken();
        } catch (XContentEOFException ignore) {
            deprecationLogger.compatibleCritical(
                STRICT_ACTION_PARSING_WARNING_KEY,
                "A bulk action wasn't closed properly with the closing brace. Malformed objects are currently accepted but will be "
                    + "rejected in a future version."
            );
            return;
        }
        if (token != XContentParser.Token.END_OBJECT) {
            deprecationLogger.compatibleCritical(
                STRICT_ACTION_PARSING_WARNING_KEY,
                "A bulk action object contained multiple keys. Additional keys are currently ignored but will be rejected in a "
                    + "future version."
            );
            return;
        }
        if (parser.nextToken() != null) {
            deprecationLogger.compatibleCritical(
                STRICT_ACTION_PARSING_WARNING_KEY,
                "A bulk action contained trailing data after the closing brace. This is currently ignored but will be rejected in a "
                    + "future version."
            );
        }
    }

    private XContentParser createParser(XContent xContent, BytesReference data) throws IOException {
        if (data.hasArray()) {
            return parseBytesArray(xContent, data, 0, data.length());
        } else {
            return xContent.createParser(config, data.streamInput());
        }
    }

    // Create an efficient parser of the given bytes, trying to directly parse a byte array if possible and falling back to stream wrapping
    // otherwise.
    private XContentParser createParser(XContent xContent, BytesReference data, int from, int nextMarker) throws IOException {
        if (data.hasArray()) {
            return parseBytesArray(xContent, data, from, nextMarker);
        } else {
            final int length = nextMarker - from;
            final BytesReference slice = data.slice(from, length);
            if (slice.hasArray()) {
                return parseBytesArray(xContent, slice, 0, length);
            } else {
                return xContent.createParser(config, slice.streamInput());
            }
        }
    }

    private XContentParser parseBytesArray(XContent xContent, BytesReference array, int from, int nextMarker) throws IOException {
        assert array.hasArray();
        final int offset = array.arrayOffset();
        return xContent.createParser(config, array.array(), offset + from, nextMarker - from);
    }
}
