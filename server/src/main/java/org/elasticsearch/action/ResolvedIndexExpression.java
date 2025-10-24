/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Set;

/**
 * This class allows capturing context about index expression replacements performed on an {@link IndicesRequest.Replaceable} during
 * index resolution, in particular the results of local resolution, and the remote (unresolved) expressions if any.
 * <p>
 * The replacements are separated into local and remote expressions.
 * For local expressions, the class allows recording local index resolution results along with failure info.
 * For remote expressions, only the expressions are recorded.
 *
 * <p>An example structure is:</p>
 *
 * <pre>{@code
 * {
 *   "original": "my-index-*",
 *   "localExpressions": {
 *     "indices": ["my-index-000001", "my-index-000002"],
 *     "localIndexResolutionResult": "SUCCESS"
 *   },
 *   "remoteExpressions": ["remote1:my-index-*", "remote2:my-index-*"]
 * }
 * }</pre>
 *
 * @param original the original index expression, as provided by the user
 * @param localExpressions the local expressions that replace the original along with their resolution result
 *                         and failure info
 * @param remoteExpressions the remote expressions that replace the original one (in the case of CPS/flat index resolution).
 *                          Only set on the local ResolvedIndexExpression, empty otherwise.
 */
public record ResolvedIndexExpression(String original, LocalExpressions localExpressions, Set<String> remoteExpressions)
    implements
        Writeable {

    public ResolvedIndexExpression(StreamInput in) throws IOException {
        this(in.readString(), new LocalExpressions(in), in.readCollectionAsImmutableSet(StreamInput::readString));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(original);
        localExpressions.writeTo(out);
        out.writeStringCollection(remoteExpressions);
    }

    /**
     * Indicates if a local index resolution attempt was successful or failed.
     * Failures can be due to concrete resources not being visible (either missing or not visible due to indices options)
     * or unauthorized concrete resources.
     * A wildcard expression resolving to nothing is still considered a successful resolution.
     * The NONE result indicates that no local resolution was attempted because the expression is known to be remote-only.
     */
    public enum LocalIndexResolutionResult {
        NONE,
        SUCCESS,
        CONCRETE_RESOURCE_NOT_VISIBLE,
        CONCRETE_RESOURCE_UNAUTHORIZED,
    }

    /**
     * Represents local (non-remote) resolution results, including expanded indices, and a {@link LocalIndexResolutionResult}.
     *
     * @param indices represents the resolved concrete indices backing the expression
     */
    public record LocalExpressions(
        Set<String> indices,
        LocalIndexResolutionResult localIndexResolutionResult,
        @Nullable ElasticsearchException exception
    ) implements Writeable {
        public LocalExpressions {
            assert localIndexResolutionResult != LocalIndexResolutionResult.SUCCESS || exception == null
                : "If the local resolution result is SUCCESS, exception must be null";
        }

        // Singleton for the case where all expressions in a ResolvedIndexExpression instance are remote
        public static final LocalExpressions NONE = new LocalExpressions(Set.of(), LocalIndexResolutionResult.NONE, null);

        public LocalExpressions(StreamInput in) throws IOException {
            this(
                in.readCollectionAsImmutableSet(StreamInput::readString),
                in.readEnum(LocalIndexResolutionResult.class),
                ElasticsearchException.readException(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(indices);
            out.writeEnum(localIndexResolutionResult);
            ElasticsearchException.writeException(exception, out);
        }
    }
}
