/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.UnsupportedValueSource;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.session.EsqlConfiguration;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.ActionListener.wrap;
import static org.elasticsearch.xpack.ql.util.DateUtils.UTC_DATE_TIME_FORMATTER;

public class TransportEsqlQueryAction extends HandledTransportAction<EsqlQueryRequest, EsqlQueryResponse> {

    private final PlanExecutor planExecutor;
    private final ComputeService computeService;
    private final ClusterService clusterService;
    private final Settings settings;

    @Inject
    public TransportEsqlQueryAction(
        Settings settings,
        TransportService transportService,
        ActionFilters actionFilters,
        PlanExecutor planExecutor,
        SearchService searchService,
        ClusterService clusterService,
        NodeClient nodeClient,
        ThreadPool threadPool,
        BigArrays bigArrays
    ) {
        super(EsqlQueryAction.NAME, transportService, actionFilters, EsqlQueryRequest::new);
        this.planExecutor = planExecutor;
        this.clusterService = clusterService;
        this.computeService = new ComputeService(searchService, clusterService, transportService, nodeClient, threadPool, bigArrays);
        this.settings = settings;
    }

    @Override
    protected void doExecute(Task task, EsqlQueryRequest request, ActionListener<EsqlQueryResponse> listener) {
        EsqlConfiguration configuration = new EsqlConfiguration(
            request.zoneId() != null ? request.zoneId() : ZoneOffset.UTC,
            // TODO: plug-in security
            null,
            clusterService.getClusterName().value(),
            request.pragmas(),
            EsqlPlugin.QUERY_RESULT_TRUNCATION_MAX_SIZE.get(settings)
        );
        planExecutor.newSession(configuration).execute(request, wrap(r -> {
            computeService.runCompute(task, r, configuration, listener.map(pages -> {
                List<ColumnInfo> columns = r.output()
                    .stream()
                    .map(c -> new ColumnInfo(c.qualifiedName(), EsqlDataTypes.outputType(c.dataType())))
                    .toList();
                return new EsqlQueryResponse(
                    columns,
                    pagesToValues(r.output().stream().map(Expression::dataType).toList(), pages),
                    request.columnar()
                );
            }));
        }, listener::onFailure));
    }

    public static List<List<Object>> pagesToValues(List<DataType> dataTypes, List<Page> pages) {
        // TODO flip this to column based by default so we do the data type comparison once per position. Row output can be rest layer.
        BytesRef scratch = new BytesRef();
        List<List<Object>> result = new ArrayList<>();
        for (Page page : pages) {
            for (int p = 0; p < page.getPositionCount(); p++) {
                List<Object> row = new ArrayList<>(page.getBlockCount());
                for (int b = 0; b < page.getBlockCount(); b++) {
                    Block block = page.getBlock(b);
                    if (block.isNull(p)) {
                        row.add(null);
                        continue;
                    }
                    /*
                     * Use the ESQL data type to map to the output to make sure compute engine
                     * respects its types. See the INTEGER clause where is doesn't always
                     * respect it.
                     */
                    if (dataTypes.get(b) == DataTypes.LONG) {
                        row.add(((LongBlock) block).getLong(p));
                        continue;
                    }
                    if (dataTypes.get(b) == DataTypes.INTEGER) {
                        row.add(((IntBlock) block).getInt(p));
                        continue;
                    }
                    if (dataTypes.get(b) == DataTypes.DOUBLE) {
                        row.add(((DoubleBlock) block).getDouble(p));
                        continue;
                    }
                    if (dataTypes.get(b) == DataTypes.KEYWORD) {
                        row.add(((BytesRefBlock) block).getBytesRef(p, scratch).utf8ToString());
                        continue;
                    }
                    if (dataTypes.get(b) == DataTypes.DATETIME) {
                        long longVal = ((LongBlock) block).getLong(p);
                        row.add(UTC_DATE_TIME_FORMATTER.formatMillis(longVal));
                        continue;
                    }
                    if (dataTypes.get(b) == DataTypes.BOOLEAN) {
                        row.add(((BooleanBlock) block).getBoolean(p));
                        continue;
                    }

                    if (dataTypes.get(b) == DataTypes.UNSUPPORTED) {
                        row.add(UnsupportedValueSource.UNSUPPORTED_OUTPUT);
                        continue;
                    }

                    throw new UnsupportedOperationException("unsupported data type [" + dataTypes.get(b) + "]");
                }
                result.add(row);
            }
        }
        return result;
    }
}
