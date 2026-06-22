/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.DatafeedTimingStatsReporter;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorQueryContext;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorUtils;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public class EsqlDataExtractor implements DataExtractor {

    private static final Logger logger = LogManager.getLogger(EsqlDataExtractor.class);

    private static final String EPOCH_MILLIS = "epoch_millis";

    private final Client client;
    private final EsqlDataExtractorContext context;
    private final DatafeedTimingStatsReporter timingStatsReporter;
    private boolean hasNext = true;
    private boolean isCancelled = false;

    public EsqlDataExtractor(Client client, EsqlDataExtractorContext context, DatafeedTimingStatsReporter timingStatsReporter) {
        this.client = Objects.requireNonNull(client);
        this.context = Objects.requireNonNull(context);
        this.timingStatsReporter = Objects.requireNonNull(timingStatsReporter);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public boolean isCancelled() {
        return isCancelled;
    }

    @Override
    public void cancel() {
        logger.trace("[{}] Data extractor received cancel request", context.jobId());
        isCancelled = true;
    }

    @Override
    public void destroy() {
        cancel();
    }

    @Override
    public long getEndTime() {
        return context.end();
    }

    @Override
    public DataSummary getSummary() {
        DataExtractorQueryContext queryContext = new DataExtractorQueryContext(
            context.indices(),
            QueryBuilders.matchAllQuery(),
            context.timeField(),
            context.start(),
            context.end(),
            context.headers(),
            context.indicesOptions(),
            Collections.emptyMap()
        );
        SearchRequestBuilder searchRequestBuilder = DataExtractorUtils.getSearchRequestBuilderForSummary(client, queryContext);
        SearchResponse searchResponse = ClientHelper.executeWithHeaders(
            context.headers(),
            ClientHelper.ML_ORIGIN,
            client,
            searchRequestBuilder::get
        );
        try {
            timingStatsReporter.reportSearchDuration(searchResponse.getTook());
            return DataExtractorUtils.getDataSummary(searchResponse);
        } finally {
            searchResponse.decRef();
        }
    }

    @Override
    public Result next() throws IOException {
        if (hasNext() == false) {
            throw new NoSuchElementException();
        }
        hasNext = false;
        SearchInterval searchInterval = new SearchInterval(context.start(), context.end());
        if (isCancelled) {
            return new Result(searchInterval, Optional.empty(), List.of());
        }

        QueryBuilder timeFilter = new RangeQueryBuilder(context.timeField()).gte(context.start()).lt(context.end()).format(EPOCH_MILLIS);

        // Anomaly detection drops records that arrive out of time order
        // (AbstractDataToProcessWriter#transformTimeAndWrite). ES|QL gives no row
        // ordering unless the query has an explicit SORT, so append one on the
        // time field. A trailing SORT overrides any user-supplied SORT, which is
        // the intended behavior for AD ingest.
        String orderedQuery = context.esqlQuery() + " | SORT `" + context.timeField() + "` ASC";

        try (EsqlQueryResponse response = runEsqlQuery(orderedQuery, timeFilter)) {
            Optional<InputStream> data = toNdjson(response.response(), context.timeField());
            return new Result(searchInterval, data, List.of());
        }
    }

    /**
     * Builds and executes the ES|QL query request. Extracted as a {@code protected} method so that unit tests can override it to avoid
     * the {@link org.elasticsearch.xpack.core.esql.action.internal.SharedSecrets} dependency that is only registered by the esql plugin
     * (which is not on the ml plugin's test classpath).
     */
    protected EsqlQueryResponse runEsqlQuery(String orderedQuery, QueryBuilder timeFilter) {
        EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request = EsqlQueryRequestBuilder
            .newRequestBuilder(client)
            .query(orderedQuery)
            .filter(timeFilter);
        return execute(request);
    }

    private static Optional<InputStream> toNdjson(EsqlResponse response, String timeField) throws IOException {
        List<? extends ColumnInfo> columns = response.columns();
        boolean[] isDateColumn = new boolean[columns.size()];
        boolean foundTimeField = false;
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).name();
            String type = columns.get(i).outputType();
            isDateColumn[i] = "date".equals(type) || "date_nanos".equals(type);
            if (timeField.equals(name)) {
                foundTimeField = true;
            }
        }
        if (foundTimeField == false) {
            throw new IllegalArgumentException(
                "ESQL query response is missing the configured time field ["
                    + timeField
                    + "]. Ensure the query's final projection includes a column named ["
                    + timeField
                    + "]. For STATS queries, alias via BY "
                    + timeField
                    + " = BUCKET("
                    + timeField
                    + ", ...)."
            );
        }
        BytesStreamOutput out = new BytesStreamOutput();
        boolean hasRows = false;
        for (Iterable<Object> row : response.rows()) {
            hasRows = true;
            try (XContentBuilder b = XContentFactory.jsonBuilder(out)) {
                b.startObject();
                int i = 0;
                for (Object value : row) {
                    int columnIndex = i++;
                    String name = columns.get(columnIndex).name();
                    if (value == null) {
                        continue;
                    }
                    if (isDateColumn[columnIndex]) {
                        if (value instanceof List<?> list) {
                            List<Long> epochMillis = new ArrayList<>(list.size());
                            for (Object element : list) {
                                epochMillis.add(toEpochMillis((String) element));
                            }
                            b.field(name, epochMillis);
                        } else {
                            b.field(name, toEpochMillis((String) value));
                        }
                    } else if (value instanceof List<?> list) {
                        b.field(name, list);
                    } else {
                        b.field(name, value);
                    }
                }
                b.endObject();
            }
            out.write('\n');
        }
        return hasRows ? Optional.of(out.bytes().streamInput()) : Optional.empty();
    }

    private static long toEpochMillis(String isoDate) {
        return Instant.parse(isoDate).toEpochMilli();
    }

    private EsqlQueryResponse execute(EsqlQueryRequestBuilder<? extends EsqlQueryRequest, ? extends EsqlQueryResponse> request) {
        return ClientHelper.executeWithHeaders(context.headers(), ClientHelper.ML_ORIGIN, client, () -> request.execute().actionGet());
    }
}
