/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver.IndexInfo;
import org.elasticsearch.xpack.sql.util.StringUtils;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.common.Strings.hasText;

public class TransportSqlListTablesAction extends HandledTransportAction<SqlListTablesRequest, SqlListTablesResponse> {
    private final SqlLicenseChecker sqlLicenseChecker;
    private final IndexResolver indexResolver;

    @Inject
    public TransportSqlListTablesAction(Settings settings, ThreadPool threadPool,
                                        TransportService transportService, ActionFilters actionFilters,
                                        IndexNameExpressionResolver indexNameExpressionResolver,
                                        SqlLicenseChecker sqlLicenseChecker, IndexResolver indexResolver) {
        super(settings, SqlListTablesAction.NAME, threadPool, transportService, actionFilters, SqlListTablesRequest::new,
                indexNameExpressionResolver);
        this.sqlLicenseChecker = sqlLicenseChecker;
        this.indexResolver = indexResolver;
    }

    @Override
    protected void doExecute(SqlListTablesRequest request, ActionListener<SqlListTablesResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());
        String indexPattern = hasText(request.getPattern()) ? StringUtils.likeToIndexWildcard(request.getPattern(), (char) 0) : "*";
        String regexPattern = hasText(request.getPattern()) ? StringUtils.likeToJavaPattern(request.getPattern(), (char) 0) : null;

        indexResolver.resolveNames(indexPattern, regexPattern, ActionListener.wrap(set -> listener.onResponse(
                new SqlListTablesResponse(set.stream()
                .map(IndexInfo::name)
                .collect(toList()))), listener::onFailure));
    }
}
