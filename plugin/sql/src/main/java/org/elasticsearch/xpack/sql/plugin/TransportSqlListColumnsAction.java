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
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.analysis.index.IndexResolver;
import org.elasticsearch.xpack.sql.type.EsField;
import org.elasticsearch.xpack.sql.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.elasticsearch.common.Strings.hasText;
import static org.elasticsearch.xpack.sql.plugin.AbstractSqlRequest.Mode.JDBC;

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
        sqlLicenseChecker.checkIfSqlAllowed(request.mode());

        String indexPattern = hasText(request.getTablePattern()) ?
                StringUtils.likeToIndexWildcard(request.getTablePattern(), (char) 0) : "*";
        String regexPattern = hasText(request.getTablePattern()) ?
                StringUtils.likeToJavaPattern(request.getTablePattern(), (char) 0) : null;

        Pattern columnMatcher = hasText(request.getColumnPattern()) ? Pattern.compile(
                StringUtils.likeToJavaPattern(request.getColumnPattern(), (char) 0)) : null;

        indexResolver.resolveAsSeparateMappings(indexPattern, regexPattern, ActionListener.wrap(esIndices -> {
            List<MetaColumnInfo> columns = new ArrayList<>();
            for (EsIndex esIndex : esIndices) {
                int pos = 0;
                for (Map.Entry<String, EsField> entry : esIndex.mapping().entrySet()) {
                    String name = entry.getKey();
                    pos++; // JDBC is 1-based so we start with 1 here
                    if (columnMatcher == null || columnMatcher.matcher(name).matches()) {
                        EsField field = entry.getValue();
                        if (request.mode() == JDBC) {
                            // the column size it's actually its precision (based on the Javadocs)
                            columns.add(new MetaColumnInfo(esIndex.name(), name, field.getDataType().esType,
                                    field.getDataType().jdbcType, field.getPrecision(), pos));
                        } else {
                            columns.add(new MetaColumnInfo(esIndex.name(), name, field.getDataType().esType, pos));
                        }
                    }
                }
            }
            listener.onResponse(new SqlListColumnsResponse(columns));
        }, listener::onFailure));
    }
}
