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
package org.elasticsearch.action.admin.indices.alias.get;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class TransportGetAliasesAction extends TransportMasterNodeReadAction<GetAliasesRequest, GetAliasesResponse> {

    @Inject
    public TransportGetAliasesAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, GetAliasesAction.NAME, transportService, clusterService, threadPool, actionFilters, GetAliasesRequest::new, indexNameExpressionResolver);
    }

    @Override
    protected String executor() {
        // very lightweight operation all in memory no need to fork to a thread pool
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected GetAliasesResponse newResponse() {
        return new GetAliasesResponse();
    }

    @Override
    protected void masterOperation(GetAliasesRequest request, ClusterState state, ActionListener<GetAliasesResponse> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        ImmutableOpenMap<String, List<AliasMetaData>> result = state.metaData().findAliases(request.aliases(), concreteIndices);

        SetOnce<String> message = new SetOnce<>();
        SetOnce<RestStatus> status = new SetOnce<>();
        if (false == Strings.isAllOrWildcard(request.aliases())) {
            String[] aliasesNames = Strings.EMPTY_ARRAY;

            if (false == Strings.isAllOrWildcard(request.aliases())) {
                aliasesNames = request.aliases();
            }

            final Set<String> aliasNames = new HashSet<>();
            for (final ObjectObjectCursor<String, List<AliasMetaData>> cursor : result) {
                for (final AliasMetaData aliasMetaData : cursor.value) {
                    aliasNames.add(aliasMetaData.alias());
                }
            }

            // first remove requested aliases that are exact matches
            final SortedSet<String> difference = Sets.sortedDifference(Arrays.stream(aliasesNames).collect(Collectors.toSet()), aliasNames);

            // now remove requested aliases that contain wildcards that are simple matches
            final List<String> matches = new ArrayList<>();
            outer:
            for (final String pattern : difference) {
                if (pattern.contains("*")) {
                    for (final String aliasName : aliasNames) {
                        if (Regex.simpleMatch(pattern, aliasName)) {
                            matches.add(pattern);
                            continue outer;
                        }
                    }
                }
            }

            difference.removeAll(matches);
            if (false == difference.isEmpty()) {
                status.set(RestStatus.NOT_FOUND);
                if (difference.size() == 1) {
                    message.set(String.format(Locale.ROOT, "alias [%s] missing", toNamesString(difference.iterator().next())));
                } else {
                    message.set(String.format(Locale.ROOT, "aliases [%s] missing", toNamesString(difference.toArray(new String[0]))));
                }
            }
        }
        if (status.get() == null) {
            status.set(RestStatus.OK);
        }
        if (message.get() == null) {
            message.set("");
        }
        listener.onResponse(new GetAliasesResponse(result, status.get(), message.get()));
    }

    private static String toNamesString(final String... names) {
        if (names == null || names.length == 0) {
            return "";
        } else if (names.length == 1) {
            return names[0];
        } else {
            return Arrays.stream(names).collect(Collectors.joining(","));
        }
    }

}
