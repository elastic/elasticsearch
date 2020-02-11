/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.rest.cat;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Table;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestResponseListener;
import org.elasticsearch.rest.action.cat.AbstractCatAction;
import org.elasticsearch.rest.action.cat.RestTable;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.TimingStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.elasticsearch.rest.RestRequest.Method.GET;

public class RestCatJobsAction extends AbstractCatAction {

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(
            new Route(GET, "_cat/ml/anomaly_detectors/{" + Job.ID.getPreferredName() + "}"),
            new Route(GET, "_cat/ml/anomaly_detectors")));
    }

    @Override
    public String getName() {
        return "cat_ml_get_jobs_action";
    }

    @Override
    protected RestChannelConsumer doCatRequest(RestRequest restRequest, NodeClient client) {
        String jobId = restRequest.param(Job.ID.getPreferredName());
        if (Strings.isNullOrEmpty(jobId)) {
            jobId = MetaData.ALL;
        }
        GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
        request.setAllowNoJobs(restRequest.paramAsBoolean(GetJobsStatsAction.Request.ALLOW_NO_JOBS.getPreferredName(),
            request.allowNoJobs()));
        return channel -> client.execute(GetJobsStatsAction.INSTANCE, request, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(GetJobsStatsAction.Response getJobStatsResponse) throws Exception {
                return RestTable.buildResponse(buildTable(restRequest, getJobStatsResponse), channel);
            }
        });
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/ml/anomaly_detectors\n");
        sb.append("/_cat/ml/anomaly_detectors/{job_id}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();

        // Job Info
        table.addCell("id", TableColumnAttributeBuilder.builder().setDescription("the job_id").build());
        table.addCell("state", TableColumnAttributeBuilder.builder()
            .setDescription("the job state")
            .setAliases("s")
            .setTextAlignment(TableColumnAttributeBuilder.TextAlign.RIGHT)
            .build());
        table.addCell("opened_time",
            TableColumnAttributeBuilder.builder()
                .setDescription("the amount of time the job has been opened")
                .setAliases("ot")
                .setDisplayByDefault(false)
                .build());
        table.addCell("assignment_explanation",
            TableColumnAttributeBuilder.builder("why the job is or is not assigned to a node", false)
                .setAliases("ae")
                .build());

        // Data Counts
        table.addCell("data.processed_records",
            TableColumnAttributeBuilder.builder("number of processed records")
            .setAliases("dpr", "dataProcessedRecords")
            .build());
        table.addCell("data.processed_fields",
            TableColumnAttributeBuilder.builder("number of processed fields", false)
                .setAliases("dpr", "dataProcessedFields")
                .build());
        table.addCell("data.input_bytes",
            TableColumnAttributeBuilder.builder("total input bytes", false)
                .setAliases("dib", "dataInputBytes")
                .build());
        table.addCell("data.input_records",
            TableColumnAttributeBuilder.builder("total record count", false)
                .setAliases("dir", "dataInputRecords")
                .build());
        table.addCell("data.input_fields",
            TableColumnAttributeBuilder.builder("total field count", false)
                .setAliases("dif", "dataInputFields")
                .build());
        table.addCell("data.invalid_dates",
            TableColumnAttributeBuilder.builder("number of records with invalid dates", false)
                .setAliases("did", "dataInvalidDates")
                .build());
        table.addCell("data.missing_fields",
            TableColumnAttributeBuilder.builder("number of records with missing fields", false)
                .setAliases("dmf", "dataMissingFields")
                .build());
        table.addCell("data.out_of_order_timestamps",
            TableColumnAttributeBuilder.builder("number of records handled out of order", false)
                .setAliases("doot", "dataOutOfOrderTimestamps")
                .build());
        table.addCell("data.empty_buckets",
            TableColumnAttributeBuilder.builder("number of empty buckets", false)
                .setAliases("deb", "dataEmptyBuckets")
                .build());
        table.addCell("data.sparse_buckets",
            TableColumnAttributeBuilder.builder("number of sparse buckets", false)
                .setAliases("dsb", "dataSparseBuckets")
                .build());
        table.addCell("data.buckets",
            TableColumnAttributeBuilder.builder("total bucket count", false)
                .setAliases("db", "dataBuckets")
                .build());
        table.addCell("data.earliest_record",
            TableColumnAttributeBuilder.builder("earliest record time", false)
                .setAliases("der", "dataEarliestRecord")
                .build());
        table.addCell("data.latest_record",
            TableColumnAttributeBuilder.builder("latest record time", false)
                .setAliases("dlr", "dataLatestRecord")
                .build());
        table.addCell("data.last",
            TableColumnAttributeBuilder.builder("last time data was seen", false)
                .setAliases("dl", "dataLast")
                .build());
        table.addCell("data.last_empty_bucket",
            TableColumnAttributeBuilder.builder("last time an empty bucket occurred", false)
                .setAliases("dleb", "dataLastEmptyBucket")
                .build());
        table.addCell("data.last_sparse_bucket",
            TableColumnAttributeBuilder.builder("last time a sparse bucket occurred", false)
                .setAliases("dlsb", "dataLastSparseBucket")
                .build());

        // Model Size stats
        table.addCell("model.bytes",
            TableColumnAttributeBuilder.builder("model size").setAliases("mb", "modelBytes").build());
        table.addCell("model.memory_status",
            TableColumnAttributeBuilder.builder("current memory status")
                .setAliases("mms", "modelMemoryStatus")
                .setTextAlignment(TableColumnAttributeBuilder.TextAlign.RIGHT)
                .build());
        table.addCell("model.bytes_exceeded",
            TableColumnAttributeBuilder.builder("how much the model has exceeded the limit", false)
                .setAliases("mbe", "modelBytesExceeded")
                .build());
        table.addCell("model.memory_limit",
            TableColumnAttributeBuilder.builder("model memory limit", false)
                .setAliases("mml", "modelMemoryLimit")
                .build());
        table.addCell("model.by_fields",
            TableColumnAttributeBuilder.builder("count of 'by' fields", false)
                .setAliases("mbf", "modelByFields")
                .build());
        table.addCell("model.over_fields",
            TableColumnAttributeBuilder.builder("count of 'over' fields", false)
                .setAliases("mof", "modelOverFields")
                .build());
        table.addCell("model.partition_fields",
            TableColumnAttributeBuilder.builder("count of 'partition' fields", false)
                .setAliases("mpf", "modelPartitionFields")
                .build());
        table.addCell("model.bucket_allocation_failures",
            TableColumnAttributeBuilder.builder("number of bucket allocation failures", false)
                .setAliases("mbaf", "modelBucketAllocationFailures")
                .build());
        table.addCell("model.categorization_status",
            TableColumnAttributeBuilder.builder("current categorization status", false)
                .setAliases("mcs", "modelCategorizationStatus")
                .setTextAlignment(TableColumnAttributeBuilder.TextAlign.RIGHT)
                .build());
        table.addCell("model.categorized_doc_count",
            TableColumnAttributeBuilder.builder("count of categorized documents", false)
                .setAliases("mcdc", "modelCategorizedDocCount")
                .build());
        table.addCell("model.total_category_count",
            TableColumnAttributeBuilder.builder("count of categories", false)
                .setAliases("mtcc", "modelTotalCategoryCount")
                .build());
        table.addCell("model.frequent_category_count",
            TableColumnAttributeBuilder.builder("count of frequent categories", false)
                .setAliases("mfcc", "modelFrequentCategoryCount")
                .build());
        table.addCell("model.rare_category_count",
            TableColumnAttributeBuilder.builder("count of rare categories", false)
                .setAliases("mrcc", "modelRareCategoryCount")
                .build());
        table.addCell("model.dead_category_count",
            TableColumnAttributeBuilder.builder("count of dead categories", false)
                .setAliases("mdcc", "modelDeadCategoryCount")
                .build());
        table.addCell("model.log_time",
            TableColumnAttributeBuilder.builder("when the model stats were gathered", false)
                .setAliases("mlt", "modelLogTime")
                .build());
        table.addCell("model.timestamp",
            TableColumnAttributeBuilder.builder("the time of the last record when the model stats were gathered", false)
                .setAliases("mt", "modelTimestamp")
                .build());

        // Forecast Stats
        table.addCell("forecast." + ForecastStats.Fields.TOTAL,
            TableColumnAttributeBuilder.builder("total number of forecasts").setAliases("ft", "forecastTotal").build());
        table.addCell("forecast.memory.min",
            TableColumnAttributeBuilder.builder("minimum memory used by forecasts", false)
                .setAliases("fmmin", "forecastMemoryMin")
                .build());
        table.addCell("forecast.memory.max",
            TableColumnAttributeBuilder.builder("maximum memory used by forecasts", false)
                .setAliases("fmmax", "forecastsMemoryMax")
                .build());
        table.addCell("forecast.memory.avg",
            TableColumnAttributeBuilder.builder("average memory used by forecasts", false)
                .setAliases("fmavg", "forecastMemoryAvg")
                .build());
        table.addCell("forecast.memory.total",
            TableColumnAttributeBuilder.builder("total memory used by all forecasts", false)
                .setAliases("fmt", "forecastMemoryTotal")
                .build());
        table.addCell("forecast." + ForecastStats.Fields.RECORDS + ".min",
            TableColumnAttributeBuilder.builder("minimum record count for forecasts", false)
                .setAliases("frmin", "forecastRecordsMin")
                .build());
        table.addCell("forecast." + ForecastStats.Fields.RECORDS + ".max",
            TableColumnAttributeBuilder.builder("maximum record count for forecasts", false)
                .setAliases("frmax", "forecastRecordsMax")
                .build());
        table.addCell("forecast." + ForecastStats.Fields.RECORDS + ".avg",
            TableColumnAttributeBuilder.builder("average record count for forecasts", false)
                .setAliases("fravg", "forecastRecordsAvg")
                .build());
        table.addCell("forecast." + ForecastStats.Fields.RECORDS + ".total",
            TableColumnAttributeBuilder.builder("total record count for all forecasts", false)
                .setAliases("frt", "forecastRecordsTotal")
                .build());
        table.addCell("forecast.time.min",
            TableColumnAttributeBuilder.builder("minimum runtime for forecasts", false)
                .setAliases("ftmin", "forecastTimeMin")
                .build());
        table.addCell("forecast.time.max",
            TableColumnAttributeBuilder.builder("maximum run time for forecasts", false)
                .setAliases("ftmax", "forecastTimeMax")
                .build());
        table.addCell("forecast.time.avg",
            TableColumnAttributeBuilder.builder("average runtime for all forecasts (milliseconds)", false)
                .setAliases("ftavg", "forecastTimeAvg")
                .build());
        table.addCell("forecast.time.total",
            TableColumnAttributeBuilder.builder("total runtime for all forecasts", false)
                .setAliases("ftt", "forecastTimeTotal").build());

        //Node info
        table.addCell("node.id",
            TableColumnAttributeBuilder.builder("id of the assigned node", false)
                .setAliases("ni", "nodeId")
                .build());
        table.addCell("node.name",
            TableColumnAttributeBuilder.builder("name of the assigned node", false)
                .setAliases("nn", "nodeName")
                .build());
        table.addCell("node.ephemeral_id",
            TableColumnAttributeBuilder.builder("ephemeral id of the assigned node", false)
                .setAliases("ne", "nodeEphemeralId")
                .build());
        table.addCell("node.address",
            TableColumnAttributeBuilder.builder("network address of the assigned node", false)
                .setAliases("na", "nodeAddress")
                .build());

        //Timing Stats
        table.addCell("bucket.count",
            TableColumnAttributeBuilder.builder("bucket count")
                .setAliases("bc", "bucketCount")
                .build());
        table.addCell("bucket.time.total",
            TableColumnAttributeBuilder.builder("total bucket processing time", false)
                .setAliases("btt", "bucketTimeTotal")
                .build());
        table.addCell("bucket.time.min",
            TableColumnAttributeBuilder.builder("minimum bucket processing time", false)
                .setAliases("btmin", "bucketTimeMin")
                .build());
        table.addCell("bucket.time.max",
            TableColumnAttributeBuilder.builder("maximum bucket processing time", false)
                .setAliases("btmax", "bucketTimeMax")
                .build());
        table.addCell("bucket.time.exp_avg",
            TableColumnAttributeBuilder.builder("exponential average bucket processing time (milliseconds)", false)
                .setAliases("btea", "bucketTimeExpAvg")
                .build());
        table.addCell("bucket.time.exp_avg_hour",
            TableColumnAttributeBuilder.builder("exponential average bucket processing time by hour (milliseconds)", false)
                .setAliases("bteah", "bucketTimeExpAvgHour")
                .build());

        table.endHeaders();
        return table;
    }

    private Table buildTable(RestRequest request, GetJobsStatsAction.Response jobStats) {
        Table table = getTableWithHeader(request);
        jobStats.getResponse().results().forEach(job -> {
            table.startRow();
            table.addCell(job.getJobId());
            table.addCell(job.getState().value());
            table.addCell(job.getOpenTime());
            table.addCell(job.getAssignmentExplanation());

            DataCounts dataCounts = job.getDataCounts();
            table.addCell(dataCounts.getProcessedRecordCount());
            table.addCell(dataCounts.getProcessedFieldCount());
            table.addCell(new ByteSizeValue(dataCounts.getInputBytes()));
            table.addCell(dataCounts.getInputRecordCount());
            table.addCell(dataCounts.getInputFieldCount());
            table.addCell(dataCounts.getInvalidDateCount());
            table.addCell(dataCounts.getMissingFieldCount());
            table.addCell(dataCounts.getOutOfOrderTimeStampCount());
            table.addCell(dataCounts.getEmptyBucketCount());
            table.addCell(dataCounts.getSparseBucketCount());
            table.addCell(dataCounts.getBucketCount());
            table.addCell(dataCounts.getEarliestRecordTimeStamp());
            table.addCell(dataCounts.getLatestRecordTimeStamp());
            table.addCell(dataCounts.getLastDataTimeStamp());
            table.addCell(dataCounts.getLatestEmptyBucketTimeStamp());
            table.addCell(dataCounts.getLatestSparseBucketTimeStamp());

            ModelSizeStats modelSizeStats = job.getModelSizeStats();
            table.addCell(modelSizeStats == null ? null : new ByteSizeValue(modelSizeStats.getModelBytes()));
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getMemoryStatus().toString());
            table.addCell(modelSizeStats == null || modelSizeStats.getModelBytesExceeded() == null ?
                null :
                new ByteSizeValue(modelSizeStats.getModelBytesExceeded()));
            table.addCell(modelSizeStats == null || modelSizeStats.getModelBytesMemoryLimit() == null ?
                null :
                new ByteSizeValue(modelSizeStats.getModelBytesMemoryLimit()));
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getTotalByFieldCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getTotalOverFieldCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getTotalPartitionFieldCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getBucketAllocationFailuresCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getCategorizationStatus().toString());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getCategorizedDocCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getTotalCategoryCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getFrequentCategoryCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getRareCategoryCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getDeadCategoryCount());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getLogTime());
            table.addCell(modelSizeStats == null ? null : modelSizeStats.getTimestamp());

            ForecastStats forecastStats = job.getForecastStats();
            boolean missingForecastStats = forecastStats == null || forecastStats.getTotal() <= 0L;
            table.addCell(forecastStats == null ? null : forecastStats.getTotal());
            table.addCell(missingForecastStats ? null : new ByteSizeValue((long)forecastStats.getMemoryStats().getMin()));
            table.addCell(missingForecastStats ? null : new ByteSizeValue((long)forecastStats.getMemoryStats().getMax()));
            table.addCell(missingForecastStats ? null : new ByteSizeValue(Math.round(forecastStats.getMemoryStats().getAvg())));
            table.addCell(missingForecastStats ? null : new ByteSizeValue((long)forecastStats.getMemoryStats().getTotal()));
            table.addCell(missingForecastStats ? null : forecastStats.getRecordStats().getMin());
            table.addCell(missingForecastStats ? null : forecastStats.getRecordStats().getMax());
            table.addCell(missingForecastStats ? null : forecastStats.getRecordStats().getAvg());
            table.addCell(missingForecastStats ? null : forecastStats.getRecordStats().getTotal());
            table.addCell(missingForecastStats ? null : TimeValue.timeValueMillis((long)forecastStats.getRuntimeStats().getMin()));
            table.addCell(missingForecastStats ? null : TimeValue.timeValueMillis((long)forecastStats.getRuntimeStats().getMax()));
            table.addCell(missingForecastStats ? null : forecastStats.getRuntimeStats().getAvg());
            table.addCell(missingForecastStats ? null : TimeValue.timeValueMillis((long)forecastStats.getRuntimeStats().getTotal()));

            DiscoveryNode node = job.getNode();
            table.addCell(node == null ? null : node.getId());
            table.addCell(node == null ? null : node.getName());
            table.addCell(node == null ? null : node.getEphemeralId());
            table.addCell(node == null ? null : node.getAddress().toString());

            TimingStats timingStats = job.getTimingStats();
            table.addCell(timingStats == null ? null : timingStats.getBucketCount());
            table.addCell(timingStats == null ? null : TimeValue.timeValueMillis((long)timingStats.getTotalBucketProcessingTimeMs()));
            table.addCell(timingStats == null || timingStats.getMinBucketProcessingTimeMs() == null ?
                null :
                TimeValue.timeValueMillis(timingStats.getMinBucketProcessingTimeMs().longValue()));
            table.addCell(timingStats == null || timingStats.getMaxBucketProcessingTimeMs() == null ?
                null :
                TimeValue.timeValueMillis(timingStats.getMaxBucketProcessingTimeMs().longValue()));
            table.addCell(timingStats == null ? null : timingStats.getExponentialAvgBucketProcessingTimeMs());
            table.addCell(timingStats == null ? null : timingStats.getExponentialAvgBucketProcessingTimePerHourMs());

            table.endRow();
        });
        return table;
    }
}
