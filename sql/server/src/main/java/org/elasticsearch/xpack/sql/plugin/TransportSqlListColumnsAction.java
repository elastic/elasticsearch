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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.common.Strings.hasText;

public class TransportSqlListColumnsAction extends HandledTransportAction<SqlListColumnsRequest, SqlListColumnsResponse> {
    private final SqlLicenseChecker sqlLicenseChecker;
    private final IndexResolver indexResolver;

    @Inject
    public TransportSqlListColumnsAction(Settings settings, ThreadPool threadPool,
                                         TransportService transportService, ActionFilters actionFilters,
                                         IndexNameExpressionResolver indexNameExpressionResolver,
                                         SqlLicenseChecker sqlLicenseChecker, IndexResolver indexResolver) {
        super(settings, SqlListColumnsAction.NAME, threadPool, transportService, actionFilters, SqlListColumnsRequest::new,
                indexNameExpressionResolver);
        this.sqlLicenseChecker = sqlLicenseChecker;
        this.indexResolver = indexResolver;
    }

    @Override
    protected void doExecute(SqlListColumnsRequest request, ActionListener<SqlListColumnsResponse> listener) {
        sqlLicenseChecker.checkIfSqlAllowed();
        // TODO: This is wrong
        // See https://github.com/elastic/x-pack-elasticsearch/pull/3438/commits/61b7c26fe08db2721f0431579f215fe493744af3
        // and https://github.com/elastic/x-pack-elasticsearch/issues/3460
        String indexPattern = Strings.hasText(request.getTablePattern()) ? StringUtils.jdbcToEsPattern(request.getTablePattern()) : "*";
        Pattern columnMatcher = hasText(request.getColumnPattern()) ? StringUtils.likeRegex(request.getColumnPattern()) : null;
        indexResolver.asList(indexPattern, ActionListener.wrap(esIndices -> {
            List<ColumnInfo> columns = new ArrayList<>();
            for (EsIndex esIndex : esIndices) {
                for (Map.Entry<String, DataType> entry : esIndex.mapping().entrySet()) {
                    String name = entry.getKey();
                    if (columnMatcher == null || columnMatcher.matcher(name).matches()) {
                        DataType type = entry.getValue();
                        // the column size it's actually its precision (based on the Javadocs)
                        columns.add(new ColumnInfo(esIndex.name(), name, type.esName(), type.sqlType(), type.displaySize()));
                    }
                }
            }
            listener.onResponse(new SqlListColumnsResponse(columns));
        }, listener::onFailure));
    }
}
