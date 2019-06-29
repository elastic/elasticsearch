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

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.rest.action.document.RestBulkAction;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;

/**
 * Helper to parse bulk requests. This should be considered an internal class.
 */
public final class BulkRequestParser {

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(LogManager.getLogger(BulkRequestParser.class));

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

    private final boolean warnOnTypeUsage;

    /**
     * Create a new parser.
     * @param warnOnTypeUsage whether it warns upon types being explicitly specified
     */
    public BulkRequestParser(boolean warnOnTypeUsage) {
        this.warnOnTypeUsage = warnOnTypeUsage;
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
    private static BytesReference sliceTrimmingCarriageReturn(BytesReference bytesReference, int from, int nextMarker,
            XContentType xContentType) {
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
            BytesReference data, @Nullable String defaultIndex,
            @Nullable String defaultRouting, @Nullable FetchSourceContext defaultFetchSourceContext,
            @Nullable String defaultPipeline, boolean allowExplicitIndex,
            XContentType xContentType,
            Consumer<IndexRequest> indexRequestConsumer,
            Consumer<UpdateRequest> updateRequestConsumer,
            Consumer<DeleteRequest> deleteRequestConsumer) throws IOException {
        parse(data, defaultIndex, null, defaultRouting, defaultFetchSourceContext, defaultPipeline, allowExplicitIndex, xContentType,
                indexRequestConsumer, updateRequestConsumer, deleteRequestConsumer);
    }

    /**
     * Parse the provided {@code data} assuming the provided default values. Index requests
     * will be passed to the {@code indexRequestConsumer}, update requests to the
     * {@code updateRequestConsumer} and delete requests to the {@code deleteRequestConsumer}.
     * @deprecated Use {@link #parse(BytesReference, String, String, FetchSourceContext, String, boolean, XContentType,
     * Consumer, Consumer, Consumer)} instead.
     */
    @Deprecated
    public void parse(
            BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType,
            @Nullable String defaultRouting, @Nullable FetchSourceContext defaultFetchSourceContext,
            @Nullable String defaultPipeline, boolean allowExplicitIndex,
            XContentType xContentType,
            Consumer<IndexRequest> indexRequestConsumer,
            Consumer<UpdateRequest> updateRequestConsumer,
            Consumer<DeleteRequest> deleteRequestConsumer) throws IOException {
        XContent xContent = xContentType.xContent();
        int line = 0;
        int from = 0;
        byte marker = xContent.streamSeparator();
        boolean typesDeprecationLogged = false;
        while (true) {
            int nextMarker = findNextMarker(marker, from, data);
            if (nextMarker == -1) {
                break;
            }
            line++;

            // now parse the action
            // EMPTY is safe here because we never call namedObject
            try (InputStream stream = data.slice(from, nextMarker - from).streamInput();
                    XContentParser parser = xContent
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)) {
                // move pointers
                from = nextMarker + 1;

                // Move to START_OBJECT
                XContentParser.Token token = parser.nextToken();
                if (token == null) {
                    continue;
                }
                if (token != XContentParser.Token.START_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                            + XContentParser.Token.START_OBJECT + " but found [" + token + "]");
                }
                // Move to FIELD_NAME, that's the action
                token = parser.nextToken();
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                            + XContentParser.Token.FIELD_NAME + " but found [" + token + "]");
                }
                String action = parser.currentName();

                String index = defaultIndex;
                String type = defaultType;
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

                // at this stage, next token can either be END_OBJECT (and use default index and type, with auto generated id)
                // or START_OBJECT which will have another set of parameters
                token = parser.nextToken();

                if (token == XContentParser.Token.START_OBJECT) {
                    String currentFieldName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (INDEX.match(currentFieldName, parser.getDeprecationHandler())){
                                if (!allowExplicitIndex) {
                                    throw new IllegalArgumentException("explicit index in bulk is not allowed");
                                }
                                index = parser.text();
                            } else if (TYPE.match(currentFieldName, parser.getDeprecationHandler())) {   
                                if (warnOnTypeUsage && typesDeprecationLogged == false) {
                                    deprecationLogger.deprecatedAndMaybeLog("bulk_with_types", RestBulkAction.TYPES_DEPRECATION_MESSAGE);
                                    typesDeprecationLogged = true;
                                }
                                type = parser.text();
                            } else if (ID.match(currentFieldName, parser.getDeprecationHandler())) {
                                id = parser.text();
                            } else if (ROUTING.match(currentFieldName, parser.getDeprecationHandler())) {
                                routing = parser.text();
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
                                pipeline = parser.text();
                            } else if (SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                                fetchSourceContext = FetchSourceContext.fromXContent(parser);
                            } else {
                                throw new IllegalArgumentException("Action/metadata line [" + line + "] contains an unknown parameter ["
                                        + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            throw new IllegalArgumentException("Malformed action/metadata line [" + line +
                                    "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                        } else if (token == XContentParser.Token.START_OBJECT && SOURCE.match(currentFieldName,
                                parser.getDeprecationHandler())) {
                            fetchSourceContext = FetchSourceContext.fromXContent(parser);
                        } else if (token != XContentParser.Token.VALUE_NULL) {
                            throw new IllegalArgumentException("Malformed action/metadata line [" + line
                                    + "], expected a simple value for field [" + currentFieldName + "] but found [" + token + "]");
                        }
                    }
                } else if (token != XContentParser.Token.END_OBJECT) {
                    throw new IllegalArgumentException("Malformed action/metadata line [" + line + "], expected "
                            + XContentParser.Token.START_OBJECT + " or " + XContentParser.Token.END_OBJECT + " but found [" + token + "]");
                }

                if ("delete".equals(action)) {
                    deleteRequestConsumer.accept(new DeleteRequest(index, type, id).routing(routing)
                            .version(version).versionType(versionType).setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm));
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
                            indexRequestConsumer.accept(new IndexRequest(index, type, id).routing(routing)
                                    .version(version).versionType(versionType)
                                    .setPipeline(pipeline).setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker,xContentType), xContentType));
                        } else {
                            indexRequestConsumer.accept(new IndexRequest(index, type, id).routing(routing)
                                    .version(version).versionType(versionType)
                                    .create("create".equals(opType)).setPipeline(pipeline)
                                    .setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                    .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType));
                        }
                    } else if ("create".equals(action)) {
                        indexRequestConsumer.accept(new IndexRequest(index, type, id).routing(routing)
                                .version(version).versionType(versionType)
                                .create(true).setPipeline(pipeline).setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                .source(sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType), xContentType));
                    } else if ("update".equals(action)) {
                        if (version != Versions.MATCH_ANY || versionType != VersionType.INTERNAL) {
                            throw new IllegalArgumentException("Update requests do not support versioning. " +
                                    "Please use `if_seq_no` and `if_primary_term` instead");
                        }
                        UpdateRequest updateRequest = new UpdateRequest(index, type, id).routing(routing).retryOnConflict(retryOnConflict)
                                .setIfSeqNo(ifSeqNo).setIfPrimaryTerm(ifPrimaryTerm)
                                .routing(routing);
                        // EMPTY is safe here because we never call namedObject
                        try (InputStream dataStream = sliceTrimmingCarriageReturn(data, from, nextMarker, xContentType).streamInput();
                                XContentParser sliceParser = xContent.createParser(NamedXContentRegistry.EMPTY,
                                        LoggingDeprecationHandler.INSTANCE, dataStream)) {
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

}
