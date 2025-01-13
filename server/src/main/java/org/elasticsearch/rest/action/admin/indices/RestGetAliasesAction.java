/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.indices;

import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.HEAD;

/**
 * The REST handler for get alias and head alias APIs.
 */
@ServerlessScope(Scope.PUBLIC)
public class RestGetAliasesAction extends BaseRestHandler {

    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT) // remove the BWC support for the deprecated ?local parameter
    private static final DeprecationLogger DEPRECATION_LOGGER = DeprecationLogger.getLogger(RestGetAliasesAction.class);

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, "/_alias"),
            new Route(GET, "/_aliases"),
            new Route(GET, "/_alias/{name}"),
            new Route(HEAD, "/_alias/{name}"),
            new Route(GET, "/{index}/_alias"),
            new Route(HEAD, "/{index}/_alias"),
            new Route(GET, "/{index}/_alias/{name}"),
            new Route(HEAD, "/{index}/_alias/{name}")
        );
    }

    @Override
    public String getName() {
        return "get_aliases_action";
    }

    static RestResponse buildRestResponse(
        boolean aliasesExplicitlyRequested,
        String[] requestedAliases,
        Map<String, List<AliasMetadata>> responseAliasMap,
        Map<String, List<DataStreamAlias>> dataStreamAliases,
        XContentBuilder builder
    ) throws Exception {
        final Set<String> indicesToDisplay = new HashSet<>();
        final Set<String> returnedAliasNames = new HashSet<>();
        for (final Map.Entry<String, List<AliasMetadata>> cursor : responseAliasMap.entrySet()) {
            for (final AliasMetadata aliasMetadata : cursor.getValue()) {
                if (aliasesExplicitlyRequested) {
                    // only display indices that have aliases
                    indicesToDisplay.add(cursor.getKey());
                }
                returnedAliasNames.add(aliasMetadata.alias());
            }
        }
        dataStreamAliases.entrySet()
            .stream()
            .flatMap(entry -> entry.getValue().stream())
            .forEach(dataStreamAlias -> returnedAliasNames.add(dataStreamAlias.getName()));

        // compute explicitly requested aliases that have are not returned in the result
        final SortedSet<String> missingAliases = new TreeSet<>();
        // first wildcard index, leading "-" as an alias name after this index means
        // that it is an exclusion
        int firstWildcardIndex = requestedAliases.length;
        for (int i = 0; i < requestedAliases.length; i++) {
            if (Regex.isSimpleMatchPattern(requestedAliases[i])) {
                firstWildcardIndex = i;
                break;
            }
        }
        for (int i = 0; i < requestedAliases.length; i++) {
            if (Metadata.ALL.equals(requestedAliases[i])
                || Regex.isSimpleMatchPattern(requestedAliases[i])
                || (i > firstWildcardIndex && requestedAliases[i].charAt(0) == '-')) {
                // only explicitly requested aliases will be called out as missing (404)
                continue;
            }
            // check if aliases[i] is subsequently excluded
            int j = Math.max(i + 1, firstWildcardIndex);
            for (; j < requestedAliases.length; j++) {
                if (requestedAliases[j].charAt(0) == '-') {
                    // this is an exclude pattern
                    if (Regex.simpleMatch(requestedAliases[j].substring(1), requestedAliases[i])
                        || Metadata.ALL.equals(requestedAliases[j].substring(1))) {
                        // aliases[i] is excluded by aliases[j]
                        break;
                    }
                }
            }
            if (j == requestedAliases.length) {
                // explicitly requested aliases[i] is not excluded by any subsequent "-" wildcard in expression
                if (false == returnedAliasNames.contains(requestedAliases[i])) {
                    // aliases[i] is not in the result set
                    missingAliases.add(requestedAliases[i]);
                }
            }
        }

        final RestStatus status;
        builder.startObject();
        {
            if (missingAliases.isEmpty()) {
                status = RestStatus.OK;
            } else {
                status = RestStatus.NOT_FOUND;
                final String message;
                if (missingAliases.size() == 1) {
                    message = String.format(Locale.ROOT, "alias [%s] missing", Strings.collectionToCommaDelimitedString(missingAliases));
                } else {
                    message = String.format(Locale.ROOT, "aliases [%s] missing", Strings.collectionToCommaDelimitedString(missingAliases));
                }
                builder.field("error", message);
                builder.field("status", status.getStatus());
            }

            for (final var entry : responseAliasMap.entrySet()) {
                if (aliasesExplicitlyRequested == false || indicesToDisplay.contains(entry.getKey())) {
                    builder.startObject(entry.getKey());
                    {
                        builder.startObject("aliases");
                        {
                            for (final AliasMetadata alias : entry.getValue()) {
                                AliasMetadata.Builder.toXContent(alias, builder, ToXContent.EMPTY_PARAMS);
                            }
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }
            // No need to do filtering like is done for aliases pointing to indices (^),
            // because this already happens in TransportGetAliasesAction.
            for (var entry : dataStreamAliases.entrySet()) {
                builder.startObject(entry.getKey());
                {
                    builder.startObject("aliases");
                    {
                        for (DataStreamAlias alias : entry.getValue()) {
                            builder.startObject(alias.getName());
                            if (entry.getKey().equals(alias.getWriteDataStream())) {
                                builder.field("is_write_index", true);
                            }
                            if (alias.getFilter(entry.getKey()) != null) {
                                builder.field(
                                    "filter",
                                    XContentHelper.convertToMap(alias.getFilter(entry.getKey()).uncompressed(), true).v2()
                                );
                            }
                            builder.endObject();
                        }
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
        }
        builder.endObject();
        return new RestResponse(status, builder);
    }

    @Override
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT) // remove the BWC support for the deprecated ?local parameter
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        // The TransportGetAliasesAction was improved do the same post processing as is happening here.
        // We can't remove this logic yet to support mixed clusters. We should be able to remove this logic here
        // in when 8.0 becomes the new version in the master branch.

        final boolean namesProvided = request.hasParam("name");
        final String[] aliases = request.paramAsStringArrayOrEmptyIfAll("name");
        final var masterNodeTimeout = RestUtils.getMasterNodeTimeout(request);
        final GetAliasesRequest getAliasesRequest = new GetAliasesRequest(masterNodeTimeout, aliases);
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        getAliasesRequest.indices(indices);
        getAliasesRequest.indicesOptions(IndicesOptions.fromRequest(request, getAliasesRequest.indicesOptions()));

        if (request.getRestApiVersion() == RestApiVersion.V_8) {
            if (request.hasParam("local")) {
                // consume this param just for validation when in BWC mode for V_8
                final var localParam = request.paramAsBoolean("local", false);
                DEPRECATION_LOGGER.critical(
                    DeprecationCategory.API,
                    "get-aliases-local",
                    "the [?local={}] query parameter to get-aliases requests has no effect and will be removed in a future version",
                    localParam
                );
            }
        }

        // we may want to move this logic to TransportGetAliasesAction but it is based on the original provided aliases, which will
        // not always be available there (they may get replaced so retrieving request.aliases is not quite the same).
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .indices()
            .getAliases(getAliasesRequest, new RestBuilderListener<>(channel) {
                @Override
                public RestResponse buildResponse(GetAliasesResponse response, XContentBuilder builder) throws Exception {
                    return buildRestResponse(namesProvided, aliases, response.getAliases(), response.getDataStreamAliases(), builder);
                }
            });
    }

}
