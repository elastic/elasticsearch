/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.index.query.WildcardQueryBuilder;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.action.util.ExpandedIdsMatcher;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedJobValidator;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.Blocked;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.MlStrings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * This class implements CRUD operation for the
 * anomaly detector job configuration document
 *
 * The number of jobs returned in a search it limited to
 * {@link MlConfigIndex#CONFIG_INDEX_MAX_RESULTS_WINDOW}.
 * In most cases we expect 10s or 100s of jobs to be defined and
 * a search for all jobs should return all.
 */
public class JobConfigProvider {

    private static final Logger logger = LogManager.getLogger(JobConfigProvider.class);

    public static final Map<String, String> TO_XCONTENT_PARAMS = Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true");

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;

    public JobConfigProvider(Client client, NamedXContentRegistry xContentRegistry) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Persist the anomaly detector job configuration to the configuration index.
     * It is an error if an job with the same Id already exists - the config will
     * not be overwritten.
     *
     * @param job The anomaly detector job configuration
     * @param listener Index response listener
     */
    public void putJob(Job job, ActionListener<IndexResponse> listener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder source = job.toXContent(builder, new ToXContent.MapParams(TO_XCONTENT_PARAMS));
            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName()).id(Job.documentId(job.getId()))
                .source(source)
                .opType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(listener::onResponse, e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException) {
                    // the job already exists
                    listener.onFailure(ExceptionsHelper.jobAlreadyExists(job.getId()));
                } else {
                    listener.onFailure(e);
                }
            }));

        } catch (IOException e) {
            listener.onFailure(new ElasticsearchParseException("Failed to serialise job with id [" + job.getId() + "]", e));
        }
    }

    /**
     * Get the anomaly detector job specified by {@code jobId}.
     * If the job is missing a {@code ResourceNotFoundException} is returned
     * via the listener.
     *
     * If the .ml-config index does not exist it is treated as a missing job
     * error.
     *
     * @param jobId The job ID
     * @param jobListener Job listener
     */
    public void getJob(String jobId, ActionListener<Job.Builder> jobListener) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), Job.documentId(jobId));

        executeAsyncWithOrigin(client.threadPool().getThreadContext(), ML_ORIGIN, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    jobListener.onFailure(ExceptionsHelper.missingJobException(jobId));
                    return;
                }

                BytesReference source = getResponse.getSourceAsBytesRef();
                parseJobLenientlyFromSource(source, jobListener);
            }

            @Override
            public void onFailure(Exception e) {
                if (e.getClass() == IndexNotFoundException.class) {
                    jobListener.onFailure(ExceptionsHelper.missingJobException(jobId));
                } else {
                    jobListener.onFailure(e);
                }
            }
        }, client::get);
    }

    /**
     * Delete the anomaly detector job config document.
     * {@code errorIfMissing} controls whether or not an error is returned
     * if the document does not exist.
     *
     * @param jobId The job id
     * @param errorIfMissing If the job document does not exist and this is true
     *                       listener fails with a ResourceNotFoundException else
     *                       the DeleteResponse is always return.
     * @param actionListener Deleted job listener
     */
    public void deleteJob(String jobId, boolean errorIfMissing, ActionListener<DeleteResponse> actionListener) {
        DeleteRequest request = new DeleteRequest(MlConfigIndex.indexName(), Job.documentId(jobId));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteAction.INSTANCE, request, actionListener.delegateFailure((l, deleteResponse) -> {
            if (errorIfMissing) {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    l.onFailure(ExceptionsHelper.missingJobException(jobId));
                    return;
                }
                assert deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
            }
            l.onResponse(deleteResponse);
        }));
    }

    /**
     * Get the job and update it by applying {@code update} then index the changed job
     * setting the version in the request. Applying the update may cause a validation error
     * which is returned via {@code updatedJobListener}
     *
     * @param jobId The Id of the job to update
     * @param update The job update
     * @param maxModelMemoryLimit The maximum model memory allowed. This can be {@code null}
     *                            if the job's {@link org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits}
     *                            are not changed.
     * @param updatedJobListener Updated job listener
     */
    public void updateJob(String jobId, JobUpdate update, ByteSizeValue maxModelMemoryLimit, ActionListener<Job> updatedJobListener) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), Job.documentId(jobId));

        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener.Delegating<>(updatedJobListener) {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    delegate.onFailure(ExceptionsHelper.missingJobException(jobId));
                    return;
                }

                final long seqNo = getResponse.getSeqNo();
                final long primaryTerm = getResponse.getPrimaryTerm();
                BytesReference source = getResponse.getSourceAsBytesRef();
                Job.Builder jobBuilder;
                try {
                    jobBuilder = parseJobLenientlyFromSource(source);
                } catch (IOException e) {
                    delegate.onFailure(new ElasticsearchParseException("Failed to parse job configuration [" + jobId + "]", e));
                    return;
                }

                Job updatedJob;
                try {
                    // Applying the update may result in a validation error
                    updatedJob = update.mergeWithJob(jobBuilder.build(), maxModelMemoryLimit);
                } catch (Exception e) {
                    delegate.onFailure(e);
                    return;
                }

                indexUpdatedJob(updatedJob, seqNo, primaryTerm, delegate);
            }
        });
    }

    /**
     * Job update validation function.
     * {@code updatedListener} must be called by implementations reporting
     * either an validation error or success.
     */
    @FunctionalInterface
    public interface UpdateValidator {
        void validate(Job job, JobUpdate update, ActionListener<Void> updatedListener);
    }

    /**
     * Similar to {@link #updateJob(String, JobUpdate, ByteSizeValue, ActionListener)} but
     * with an extra validation step which is called before the updated is applied.
     *
     * @param jobId The Id of the job to update
     * @param update The job update
     * @param maxModelMemoryLimit The maximum model memory allowed
     * @param validator The job update validator
     * @param listener Updated job listener
     */
    public void updateJobWithValidation(
        String jobId,
        JobUpdate update,
        ByteSizeValue maxModelMemoryLimit,
        UpdateValidator validator,
        ActionListener<Job> listener
    ) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), Job.documentId(jobId));

        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse.isExists() == false) {
                listener.onFailure(ExceptionsHelper.missingJobException(jobId));
                return;
            }

            final long seqNo = getResponse.getSeqNo();
            final long primaryTerm = getResponse.getPrimaryTerm();
            BytesReference source = getResponse.getSourceAsBytesRef();
            Job originalJob;
            try {
                originalJob = parseJobLenientlyFromSource(source).build();
            } catch (Exception e) {
                listener.onFailure(new ElasticsearchParseException("Failed to parse job configuration [" + jobId + "]", e));
                return;
            }

            validator.validate(originalJob, update, ActionListener.wrap(validated -> {
                Job updatedJob;
                try {
                    // Applying the update may result in a validation error
                    updatedJob = update.mergeWithJob(originalJob, maxModelMemoryLimit);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }

                indexUpdatedJob(updatedJob, seqNo, primaryTerm, listener);
            }, listener::onFailure));
        }, e -> {
            if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                listener.onFailure(ExceptionsHelper.missingJobException(jobId));
            } else {
                listener.onFailure(e);
            }
        }));
    }

    private void indexUpdatedJob(Job updatedJob, long seqNo, long primaryTerm, ActionListener<Job> updatedJobListener) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            XContentBuilder updatedSource = updatedJob.toXContent(builder, ToXContent.EMPTY_PARAMS);
            IndexRequest indexRequest = new IndexRequest(MlConfigIndex.indexName()).id(Job.documentId(updatedJob.getId()))
                .source(updatedSource)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.setIfSeqNo(seqNo);
            indexRequest.setIfPrimaryTerm(primaryTerm);

            executeAsyncWithOrigin(client, ML_ORIGIN, IndexAction.INSTANCE, indexRequest, ActionListener.wrap(indexResponse -> {
                assert indexResponse.getResult() == DocWriteResponse.Result.UPDATED;
                updatedJobListener.onResponse(updatedJob);
            }, updatedJobListener::onFailure));

        } catch (IOException e) {
            updatedJobListener.onFailure(
                new ElasticsearchParseException("Failed to serialise job with id [" + updatedJob.getId() + "]", e)
            );
        }
    }

    /**
     * Check a job exists. A job exists if it has a configuration document.
     * If the .ml-config index does not exist it is treated as a missing job
     * error.
     *
     * Depending on the value of {@code errorIfMissing} if the job does not
     * exist a ResourceNotFoundException is returned to the listener,
     * otherwise false is returned in the response.
     *
     * @param jobId             The jobId to check
     * @param errorIfMissing    If true and the job is missing the listener fails with
     *                          a ResourceNotFoundException else false is returned.
     * @param listener          Exists listener
     */
    public void jobExists(String jobId, boolean errorIfMissing, ActionListener<Boolean> listener) {
        GetRequest getRequest = new GetRequest(MlConfigIndex.indexName(), Job.documentId(jobId));
        getRequest.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);

        executeAsyncWithOrigin(client, ML_ORIGIN, GetAction.INSTANCE, getRequest, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                if (getResponse.isExists() == false) {
                    if (errorIfMissing) {
                        listener.onFailure(ExceptionsHelper.missingJobException(jobId));
                    } else {
                        listener.onResponse(Boolean.FALSE);
                    }
                } else {
                    listener.onResponse(Boolean.TRUE);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (e.getClass() == IndexNotFoundException.class) {
                    if (errorIfMissing) {
                        listener.onFailure(ExceptionsHelper.missingJobException(jobId));
                    } else {
                        listener.onResponse(Boolean.FALSE);
                    }
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    /**
     * For the list of job Ids find all that match existing jobs Ids.
     * The repsonse is all the job Ids in {@code ids} that match an existing
     * job Id.
     * @param ids Job Ids to find
     * @param listener The matched Ids listener
     */
    public void jobIdMatches(List<String> ids, ActionListener<List<String>> listener) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder(Job.JOB_TYPE.getPreferredName(), Job.ANOMALY_DETECTOR_JOB_TYPE));
        boolQueryBuilder.filter(new TermsQueryBuilder(Job.ID.getPreferredName(), ids));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder);
        sourceBuilder.fetchSource(false);
        sourceBuilder.docValueField(Job.ID.getPreferredName(), null);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .setSize(ids.size())
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                SearchHit[] hits = response.getHits().getHits();
                List<String> matchedIds = new ArrayList<>();
                for (SearchHit hit : hits) {
                    matchedIds.add(hit.field(Job.ID.getPreferredName()).getValue());
                }
                listener.onResponse(matchedIds);
            }, listener::onFailure),
            client::search
        );
    }

    public void updateJobBlockReason(String jobId, Blocked blocked, ActionListener<PutJobAction.Response> listener) {
        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setBlocked(blocked).build();
        executeAsyncWithOrigin(client, ML_ORIGIN, UpdateJobAction.INSTANCE, UpdateJobAction.Request.internal(jobId, jobUpdate), listener);
    }

    public void updateJobAfterReset(String jobId, ActionListener<PutJobAction.Response> listener) {
        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setModelSnapshotId(ModelSnapshot.EMPTY_SNAPSHOT_ID)
            .setBlocked(Blocked.none())
            .setClearFinishTime(true)
            .build();
        executeAsyncWithOrigin(client, ML_ORIGIN, UpdateJobAction.INSTANCE, UpdateJobAction.Request.internal(jobId, jobUpdate), listener);
    }

    /**
     * Expands an expression into the set of matching names. {@code expresssion}
     * may be a wildcard, a job group, a job Id or a list of those.
     * If {@code expression} == 'ALL', '*' or the empty string then all
     * job Ids are returned.
     * Job groups are expanded to all the jobs Ids in that group.
     *
     * If {@code expression} contains a job Id or a Group name then it
     * is an error if the job or group do not exist.
     *
     * For example, given a set of names ["foo-1", "foo-2", "bar-1", bar-2"],
     * expressions resolve follows:
     * <ul>
     *     <li>"foo-1" : ["foo-1"]</li>
     *     <li>"bar-1" : ["bar-1"]</li>
     *     <li>"foo-1,foo-2" : ["foo-1", "foo-2"]</li>
     *     <li>"foo-*" : ["foo-1", "foo-2"]</li>
     *     <li>"*-1" : ["bar-1", "foo-1"]</li>
     *     <li>"*" : ["bar-1", "bar-2", "foo-1", "foo-2"]</li>
     *     <li>"_all" : ["bar-1", "bar-2", "foo-1", "foo-2"]</li>
     * </ul>
     *
     * @param expression the expression to resolve
     * @param allowNoMatch if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param excludeDeleting If true exclude jobs marked as deleting
     * @param tasksCustomMetadata The current persistent task metadata.
     *                            For resolving jobIds that have tasks, but for some reason, don't have configs
     * @param allowMissingConfigs If a job has a task, but is missing a config, allow the ID to be expanded via the existing task
     * @param listener The expanded job Ids listener
     */
    public void expandJobsIds(
        String expression,
        boolean allowNoMatch,
        boolean excludeDeleting,
        @Nullable PersistentTasksCustomMetadata tasksCustomMetadata,
        boolean allowMissingConfigs,
        ActionListener<SortedSet<String>> listener
    ) {
        String[] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildJobWildcardQuery(tokens, excludeDeleting));
        sourceBuilder.sort(Job.ID.getPreferredName());
        sourceBuilder.fetchSource(false);
        sourceBuilder.docValueField(Job.ID.getPreferredName(), null);
        sourceBuilder.docValueField(Job.GROUPS.getPreferredName(), null);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .setSize(MlConfigIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
            .request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoMatch);
        Collection<String> openMatchingJobs = matchingJobIdsWithTasks(tokens, tasksCustomMetadata);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                SortedSet<String> jobIds = new TreeSet<>();
                SortedSet<String> groupsIds = new TreeSet<>();
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    jobIds.add(hit.field(Job.ID.getPreferredName()).getValue());
                    List<Object> groups = hit.field(Job.GROUPS.getPreferredName()).getValues();
                    if (groups != null) {
                        groupsIds.addAll(groups.stream().map(Object::toString).collect(Collectors.toList()));
                    }
                }
                if (allowMissingConfigs) {
                    jobIds.addAll(openMatchingJobs);
                }
                groupsIds.addAll(jobIds);
                requiredMatches.filterMatchedIds(groupsIds);
                if (requiredMatches.hasUnmatchedIds()) {
                    // some required jobs were not found
                    listener.onFailure(ExceptionsHelper.missingJobException(requiredMatches.unmatchedIdsString()));
                    return;
                }

                listener.onResponse(jobIds);
            }, listener::onFailure),
            client::search
        );

    }

    /**
     * The same logic as {@link #expandJobsIds(String, boolean, boolean, PersistentTasksCustomMetadata, boolean, ActionListener)} but
     * the full anomaly detector job configuration is returned.
     *
     * See {@link #expandJobsIds(String, boolean, boolean, PersistentTasksCustomMetadata, boolean, ActionListener)}
     *
     * @param expression the expression to resolve
     * @param allowNoMatch if {@code false}, an error is thrown when no name matches the {@code expression}.
     *                     This only applies to wild card expressions, if {@code expression} is not a
     *                     wildcard then setting this true will not suppress the exception
     * @param excludeDeleting If true exclude jobs marked as deleting
     * @param listener The expanded jobs listener
     */
    public void expandJobs(String expression, boolean allowNoMatch, boolean excludeDeleting, ActionListener<List<Job.Builder>> listener) {
        String[] tokens = ExpandedIdsMatcher.tokenizeExpression(expression);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(buildJobWildcardQuery(tokens, excludeDeleting));
        sourceBuilder.sort(Job.ID.getPreferredName());

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .setSize(MlConfigIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
            .request();

        ExpandedIdsMatcher requiredMatches = new ExpandedIdsMatcher(tokens, allowNoMatch);

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                List<Job.Builder> jobs = new ArrayList<>();
                Set<String> jobAndGroupIds = new HashSet<>();

                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    try {
                        BytesReference source = hit.getSourceRef();
                        Job.Builder job = parseJobLenientlyFromSource(source);
                        jobs.add(job);
                        jobAndGroupIds.add(job.getId());
                        jobAndGroupIds.addAll(job.getGroups());
                    } catch (IOException e) {
                        // TODO A better way to handle this rather than just ignoring the error?
                        logger.error("Error parsing anomaly detector job configuration [" + hit.getId() + "]", e);
                    }
                }

                requiredMatches.filterMatchedIds(jobAndGroupIds);
                if (requiredMatches.hasUnmatchedIds()) {
                    // some required jobs were not found
                    listener.onFailure(ExceptionsHelper.missingJobException(requiredMatches.unmatchedIdsString()));
                    return;
                }

                listener.onResponse(jobs);
            }, listener::onFailure),
            client::search
        );

    }

    /**
     * Expands the list of job group Ids to the set of jobs which are members of the groups.
     * Unlike {@link #expandJobsIds(String, boolean, boolean, PersistentTasksCustomMetadata, boolean, ActionListener)} it is not an error
     * if a group Id does not exist.
     * Wildcard expansion of group Ids is not supported.
     *
     * @param groupIds Group Ids to expand
     * @param listener Expanded job Ids listener
     */
    public void expandGroupIds(List<String> groupIds, ActionListener<SortedSet<String>> listener) {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new TermsQueryBuilder(Job.GROUPS.getPreferredName(), groupIds));
        sourceBuilder.sort(Job.ID.getPreferredName(), SortOrder.DESC);
        sourceBuilder.fetchSource(false);
        sourceBuilder.docValueField(Job.ID.getPreferredName(), null);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .setSize(MlConfigIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                SortedSet<String> jobIds = new TreeSet<>();
                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    jobIds.add(hit.field(Job.ID.getPreferredName()).getValue());
                }

                listener.onResponse(jobIds);
            }, listener::onFailure),
            client::search
        );
    }

    /**
     * Check if a group exists, that is there exists a job that is a member of
     * the group. If there are one or more jobs that define the group then
     * the listener responds with true else false.
     *
     * @param groupId The group Id
     * @param listener Returns true, false or a failure
     */
    public void groupExists(String groupId, ActionListener<Boolean> listener) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder(Job.JOB_TYPE.getPreferredName(), Job.ANOMALY_DETECTOR_JOB_TYPE));
        boolQueryBuilder.filter(new TermQueryBuilder(Job.GROUPS.getPreferredName(), groupId));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(boolQueryBuilder);
        sourceBuilder.fetchSource(false);

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setSize(0)
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(
                response -> { listener.onResponse(response.getHits().getTotalHits().value > 0); },
                listener::onFailure
            ),
            client::search
        );
    }

    /**
     * Find jobs with custom rules defined.
     * @param listener Jobs listener
     */
    public void findJobsWithCustomRules(ActionListener<List<Job>> listener) {
        String customRulesPath = Strings.collectionToDelimitedString(
            Arrays.asList(
                Job.ANALYSIS_CONFIG.getPreferredName(),
                AnalysisConfig.DETECTORS.getPreferredName(),
                Detector.CUSTOM_RULES_FIELD.getPreferredName()
            ),
            "."
        );
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(
            QueryBuilders.nestedQuery(customRulesPath, QueryBuilders.existsQuery(customRulesPath), ScoreMode.None)
        );

        SearchRequest searchRequest = client.prepareSearch(MlConfigIndex.indexName())
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .setSource(sourceBuilder)
            .setSize(MlConfigIndex.CONFIG_INDEX_MAX_RESULTS_WINDOW)
            .request();

        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ML_ORIGIN,
            searchRequest,
            ActionListener.<SearchResponse>wrap(response -> {
                List<Job> jobs = new ArrayList<>();

                SearchHit[] hits = response.getHits().getHits();
                for (SearchHit hit : hits) {
                    try {
                        BytesReference source = hit.getSourceRef();
                        Job job = parseJobLenientlyFromSource(source).build();
                        jobs.add(job);
                    } catch (IOException e) {
                        // TODO A better way to handle this rather than just ignoring the error?
                        logger.error("Error parsing anomaly detector job configuration [" + hit.getId() + "]", e);
                    }
                }

                listener.onResponse(jobs);
            }, listener::onFailure),
            client::search
        );
    }

    /**
     * Get the job reference by the datafeed and validate the datafeed config against it
     * @param config  Datafeed config
     * @param listener Validation listener
     */
    public void validateDatafeedJob(DatafeedConfig config, ActionListener<Boolean> listener) {
        getJob(config.getJobId(), ActionListener.wrap(jobBuilder -> {
            try {
                DatafeedJobValidator.validate(config, jobBuilder.build(), xContentRegistry);
                listener.onResponse(Boolean.TRUE);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }, listener::onFailure));
    }

    static Collection<String> matchingJobIdsWithTasks(String[] jobIdPatterns, PersistentTasksCustomMetadata tasksMetadata) {
        return MlStrings.findMatching(jobIdPatterns, MlTasks.openJobIds(tasksMetadata));
    }

    private void parseJobLenientlyFromSource(BytesReference source, ActionListener<Job.Builder> jobListener) {
        try (
            InputStream stream = source.streamInput();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            jobListener.onResponse(Job.LENIENT_PARSER.apply(parser, null));
        } catch (Exception e) {
            jobListener.onFailure(e);
        }
    }

    private Job.Builder parseJobLenientlyFromSource(BytesReference source) throws IOException {
        try (
            InputStream stream = source.streamInput();
            XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            return Job.LENIENT_PARSER.apply(parser, null);
        }
    }

    public static QueryBuilder buildJobWildcardQuery(String[] tokens, boolean excludeDeleting) {
        QueryBuilder jobQuery = new TermQueryBuilder(Job.JOB_TYPE.getPreferredName(), Job.ANOMALY_DETECTOR_JOB_TYPE);
        if (Strings.isAllOrWildcard(tokens) && excludeDeleting == false) {
            // match all
            return jobQuery;
        }

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(jobQuery);
        BoolQueryBuilder shouldQueries = new BoolQueryBuilder();

        if (excludeDeleting) {
            // field exists only when the job is marked as deleting
            shouldQueries.mustNot(new ExistsQueryBuilder(Job.DELETING.getPreferredName()));

            if (Strings.isAllOrWildcard(tokens)) {
                boolQueryBuilder.filter(shouldQueries);
                return boolQueryBuilder;
            }
        }

        List<String> terms = new ArrayList<>();
        for (String token : tokens) {
            if (Regex.isSimpleMatchPattern(token)) {
                shouldQueries.should(new WildcardQueryBuilder(Job.ID.getPreferredName(), token));
                shouldQueries.should(new WildcardQueryBuilder(Job.GROUPS.getPreferredName(), token));
            } else {
                terms.add(token);
            }
        }

        if (terms.isEmpty() == false) {
            shouldQueries.should(new TermsQueryBuilder(Job.ID.getPreferredName(), terms));
            shouldQueries.should(new TermsQueryBuilder(Job.GROUPS.getPreferredName(), terms));
        }

        if (shouldQueries.should().isEmpty() == false) {
            boolQueryBuilder.filter(shouldQueries);
        }

        return boolQueryBuilder;
    }
}
