/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.textstructure.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.textstructure.action.FindFieldStructureAction;
import org.elasticsearch.xpack.core.textstructure.action.TextStructureResponse;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinder;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureFinderManager;
import org.elasticsearch.xpack.textstructure.structurefinder.TextStructureOverrides;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.threadpool.ThreadPool.Names.GENERIC;

public class TransportFindFieldStructureAction extends HandledTransportAction<FindFieldStructureAction.Request, TextStructureResponse> {

    private final ThreadPool threadPool;
    private final XPackLicenseState licenseState;
    private final Client client;
    private final SecurityContext securityContext;

    @Inject
    public TransportFindFieldStructureAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        XPackLicenseState licenseState,
        Client client,
        Settings settings
    ) {
        super(FindFieldStructureAction.NAME, transportService, actionFilters, FindFieldStructureAction.Request::new);
        this.threadPool = threadPool;
        this.licenseState = licenseState;
        this.client = client;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
    }

    @Override
    protected void doExecute(Task task, FindFieldStructureAction.Request request, ActionListener<TextStructureResponse> listener) {

        if (licenseState.isSecurityEnabled()) {
            final String[] indices = request.getIndices();

            final String username = securityContext.getUser().principal();
            final HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
            privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
            privRequest.username(username);
            privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
            privRequest.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder().indices(indices).privileges(SearchAction.NAME).build());

            ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                r -> handlePrivsResponse(username, request, r, listener),
                listener::onFailure
            );
            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);

        } else {
            searchAndBuildTextStructureResponse(request, threadPool.getThreadContext().getHeaders(), listener);
        }

    }

    private void handlePrivsResponse(
        String username,
        FindFieldStructureAction.Request request,
        HasPrivilegesResponse response,
        ActionListener<TextStructureResponse> listener
    ) throws IOException {
        if (response.isCompleteMatch()) {
            searchAndBuildTextStructureResponse(request, threadPool.getThreadContext().getHeaders(), listener);
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            for (ResourcePrivileges index : response.getIndexPrivileges()) {
                builder.field(index.getResource());
                builder.map(index.getPrivileges());
            }
            builder.endObject();

            listener.onFailure(
                Exceptions.authorizationError(
                    "Cannot determine field structure because user {} lacks permissions on the indices: {}",
                    username,
                    Strings.toString(builder)
                )
            );
        }
    }

    private void searchAndBuildTextStructureResponse(
        FindFieldStructureAction.Request request,
        Map<String, String> headers,
        ActionListener<TextStructureResponse> listener
    ) {

        ActionListener<SearchResponse> searchResponseActionListener = ActionListener.wrap(searchResponse -> {
            // TODO could we just feed the doc fields as "lines" to the field structure finder?
            StringBuilder stringBuilder = new StringBuilder();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                if (request.getRuntimeMappings().isEmpty()) {
                    Object value = MapHelper.dig(request.getFieldName(), Objects.requireNonNull(hit.getSourceAsMap()));
                    if (value instanceof String) {
                        stringBuilder.append(value.toString()).append("\n");
                    } else if (value instanceof List<?>) {
                        @SuppressWarnings("unchecked")
                        List<Object> values = (List<Object>) value;
                        for (Object v : values) {
                            if (v instanceof String) {
                                stringBuilder.append(v.toString()).append("\n");
                            }
                        }
                    }
                } else {
                    DocumentField field = hit.field(request.getFieldName());
                    for (Object v : field.getValues()) {
                        if (v instanceof String) {
                            stringBuilder.append(v.toString()).append("\n");
                        }
                    }
                }
            }
            threadPool.executor(GENERIC).execute(() -> {
                try {
                    listener.onResponse(buildTextStructureResponse(request, new BytesArray(stringBuilder.toString())));
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
        }, listener::onFailure);
        FunctionScoreQueryBuilder functionBuilder = request.getQueryBuilder() != null
            ? QueryBuilders.functionScoreQuery(request.getQueryBuilder(), ScoreFunctionBuilders.randomFunction())
            : QueryBuilders.functionScoreQuery(ScoreFunctionBuilders.randomFunction());

        SearchRequestBuilder searchRequest = client.prepareSearch(request.getIndices())
            .setQuery(functionBuilder)
            .setSize(request.getLinesToSample())
            .setTrackTotalHits(false);
        // If we have a runtime mapping, that means we have to fetch via the fields
        // But, don't do that unless we have to so that the user could query older remote clusters
        if (request.getRuntimeMappings().isEmpty() == false) {
            searchRequest.addFetchField(request.getFieldName()).setFetchSource(false).setRuntimeMappings(request.getRuntimeMappings());
        } else {
            searchRequest.setFetchSource(new String[] { request.getFieldName() }, null);
        }

        ClientHelper.executeWithHeadersAsync(
            headers,
            ClientHelper.TEXT_STRUCTURE_ORIGIN,
            client,
            SearchAction.INSTANCE,
            searchRequest.request(),
            searchResponseActionListener
        );
    }

    private TextStructureResponse buildTextStructureResponse(FindFieldStructureAction.Request request, BytesReference sample)
        throws Exception {

        TextStructureFinderManager structureFinderManager = new TextStructureFinderManager(threadPool.scheduler());
        try (InputStream sampleStream = sample.streamInput()) {
            TextStructureFinder textStructureFinder = structureFinderManager.findTextStructure(
                request.getLinesToSample(),
                request.getLineMergeSizeLimit(),
                sampleStream,
                new TextStructureOverrides(request),
                request.getTimeout()
            );
            return new TextStructureResponse(textStructureFinder.getStructure());
        }
    }
}
