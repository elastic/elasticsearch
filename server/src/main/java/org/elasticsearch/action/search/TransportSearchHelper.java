/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.VersionCheckingStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalScrollSearchRequest;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Base64;
import java.util.List;

public final class TransportSearchHelper {

    private static final String INCLUDE_CONTEXT_UUID = "include_context_uuid";

    static InternalScrollSearchRequest internalScrollSearchRequest(ShardSearchContextId id, SearchScrollRequest request) {
        return new InternalScrollSearchRequest(request, id);
    }

    static String buildScrollId(AtomicArray<? extends SearchPhaseResult> searchPhaseResults) {
        try {
            BytesStreamOutput out = new BytesStreamOutput();
            out.writeString(INCLUDE_CONTEXT_UUID);
            out.writeString(searchPhaseResults.length() == 1 ? ParsedScrollId.QUERY_AND_FETCH_TYPE : ParsedScrollId.QUERY_THEN_FETCH_TYPE);
            out.writeVInt(searchPhaseResults.asList().size());
            for (SearchPhaseResult searchPhaseResult : searchPhaseResults.asList()) {
                out.writeString(searchPhaseResult.getContextId().getSessionId());
                out.writeLong(searchPhaseResult.getContextId().getId());
                SearchShardTarget searchShardTarget = searchPhaseResult.getSearchShardTarget();
                if (searchShardTarget.getClusterAlias() != null) {
                    out.writeString(
                        RemoteClusterAware.buildRemoteIndexName(searchShardTarget.getClusterAlias(), searchShardTarget.getNodeId())
                    );
                } else {
                    out.writeString(searchShardTarget.getNodeId());
                }
            }
            return Base64.getUrlEncoder().encodeToString(out.copyBytes().array());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static ParsedScrollId parseScrollId(String scrollId) {
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(scrollId);
            ByteArrayStreamInput in = new ByteArrayStreamInput(bytes);
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
            SearchContextIdForNode[] context = new SearchContextIdForNode[in.readVInt()];
            for (int i = 0; i < context.length; ++i) {
                final String contextUUID = includeContextUUID ? in.readString() : "";
                long id = in.readLong();
                String target = in.readString();
                String clusterAlias;
                final int index = target.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR);
                if (index == -1) {
                    clusterAlias = null;
                } else {
                    clusterAlias = target.substring(0, index);
                    target = target.substring(index + 1);
                }
                context[i] = new SearchContextIdForNode(clusterAlias, target, new ShardSearchContextId(contextUUID, id));
            }
            if (in.getPosition() != bytes.length) {
                throw new IllegalArgumentException("Not all bytes were read");
            }
            return new ParsedScrollId(scrollId, type, context);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse scroll id", e);
        }
    }

    private static final List<Version> ALL_VERSIONS = Version.getDeclaredVersions(Version.class);
    private static final Version CCS_CHECK_VERSION = getPreviousMinorSeries(Version.CURRENT);

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
            writeableRequest.writeTo(new VersionCheckingStreamOutput(CCS_CHECK_VERSION));
        } catch (Exception e) {
            // if we cannot serialize, raise this as an error to indicate to the caller that CCS has problems with this request
            throw new IllegalArgumentException(
                "["
                    + writeableRequest.getClass()
                    + "] is not compatible with version "
                    + CCS_CHECK_VERSION
                    + " and the '"
                    + SearchService.CCS_VERSION_CHECK_SETTING.getKey()
                    + "' setting is enabled.",
                e
            );
        }
    }

    /**
     * Returns the first minor version previous to the minor version passed in.
     * I.e 8.2.1 will return 8.1.0
     */
    static Version getPreviousMinorSeries(Version current) {
        for (int i = ALL_VERSIONS.size() - 1; i >= 0; i--) {
            Version v = ALL_VERSIONS.get(i);
            if (v.before(current) && (v.minor < current.minor || v.major < current.major)) {
                return Version.fromId(v.major * 1000000 + v.minor * 10000 + 99);
            }
        }
        throw new IllegalArgumentException("couldn't find any released versions of the minor before [" + current + "]");
    }

    private TransportSearchHelper() {

    }
}
