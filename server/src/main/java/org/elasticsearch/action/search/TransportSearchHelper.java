/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.VersionCheckingStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class TransportSearchHelper {

    private static final String INCLUDE_CONTEXT_UUID = "include_context_uuid";

    static InternalScrollSearchRequest internalScrollSearchRequest(ShardSearchContextId id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }

    static String buildScrollId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults) {
        final BytesReference bytesReference;
        try (var encodedStreamOutput = new BytesStreamOutput()) {
            try (var out = new OutputStreamStreamOutput(Base64.getUrlEncoder().wrap(encodedStreamOutput))) {
                out.writeString(INCLUDE_CONTEXT_UUID);
                out.writeString(
                    searchPhaseResults.length() == 1 ? ParsedScrollId.QUERY_AND_FETCH_TYPE : ParsedScrollId.QUERY_THEN_FETCH_TYPE
                );
                out.writeCollection(searchPhaseResults.asList(), (o, searchPhaseResult) -> {
                    o.writeString(searchPhaseResult.getContextId().getSessionId());
                    o.writeLong(searchPhaseResult.getContextId().getId());
                    SearchShardTarget searchShardTarget = searchPhaseResult.getSearchShardTarget();
                    if (searchShardTarget.getClusterAlias() != null) {
                        o.writeString(
                            RemoteClusterAware.buildRemoteIndexName(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId())
                        );
                    } else {
                        o.writeString(searchShardTarget.getNodeId());
                    }
                });
            }
            bytesReference = encodedStreamOutput.bytes();
        } catch (IOException e) {
            assert false : e;
            throw new UncheckedIOException(e);
        }
        final BytesRef bytesRef = bytesReference.toBytesRef();
        return new String(bytesRef.bytes, bytesRef.offset, bytesRef.length, StandardCharsets.ISO_8859_1);
    }

    static ParsedScrollId parseScrollId(String scrollId) {
        try (
            var decodedInputStream = Base64.getUrlDecoder().wrap(new ByteArrayInputStream(scrollId.getBytes(StandardCharsets.ISO_8859_1)));
            var in = new InputStreamStreamInput(decodedInputStream)
        ) {
            final boolean includeContextUUID;
            final String type;
            final String firstChunk = in.readString();
            if (INCLUDE_CONTEXT_UUID.equals(firstChunk)) {
                includeContextUUID = true;
                type = in.readString();
            } else {
                includeContextUUID = false;
                type = firstChunk;
            }
            final SearchContextIdForNode[] context = in.readArray(
                includeContextUUID
                    ? TransportSearchHelper::readSearchContextIdForNodeIncludingContextUUID
                    : TransportSearchHelper::readSearchContextIdForNodeExcludingContextUUID,
                SearchContextIdForNode[]::new
            );
            if (in.available() > 0) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new ParsedScrollId(type, context);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse scroll id", e);
        }
    }

    private static SearchContextIdForNode readSearchContextIdForNodeIncludingContextUUID(StreamInput in) throws IOException {
        return innerReadSearchContextIdForNode(in.readString(), in);
    }

    private static SearchContextIdForNode readSearchContextIdForNodeExcludingContextUUID(StreamInput in) throws IOException {
        return innerReadSearchContextIdForNode("", in);
    }

    private static SearchContextIdForNode innerReadSearchContextIdForNode(String contextUUID, StreamInput in) throws IOException {
        long id = in.readLong();
        String[] split = RemoteClusterAware.splitIndexName(in.readString());
        String clusterAlias = split[0];
        String target = split[1];
        return new SearchContextIdForNode(clusterAlias, target, new ShardSearchContextId(contextUUID, id));
    }

    /**
    * Using the 'search.check_ccs_compatibility' setting, clients can ask for an early
    * check that inspects the incoming request and tries to verify that it can be handled by
    * a CCS compliant earlier version, e.g. currently a N-1 version where N is the current minor.
    *
    * Checking the compatibility involved serializing the request to a stream output that acts like
    * it was on the previous minor version. This should e.g. trigger errors for {@link Writeable} parts of
    * the requests that were not available in those versions.
    */
    public static void checkCCSVersionCompatibility(Writeable writeableRequest) {
        try {
            writeableRequest.writeTo(new VersionCheckingStreamOutput(TransportVersions.MINIMUM_CCS_VERSION));
        } catch (Exception e) {
            // if we cannot serialize, raise this as an error to indicate to the caller that CCS has problems with this request
            throw new IllegalArgumentException(
                "["
                    + writeableRequest.getClass()
                    + "] is not compatible with version "
                    + TransportVersions.MINIMUM_CCS_VERSION.toReleaseVersion()
                    + " and the '"
                    + SearchService.CCS_VERSION_CHECK_SETTING.getKey()
                    + "' setting is enabled.",
                e
            );
        }
    }

    private TransportSearchHelper() {}
}
