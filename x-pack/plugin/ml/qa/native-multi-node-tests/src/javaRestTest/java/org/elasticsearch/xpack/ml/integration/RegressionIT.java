/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.NodeAcknowledgedResponse;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.BoostedTreeParams;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelDefinition;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.xpack.core.ml.inference.preprocessing.PreProcessor;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble.Ensemble;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.Hyperparameters;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.metadata.TrainedModelMetadata;
import org.hamcrest.Matchers;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class RegressionIT extends MlNativeDataFrameAnalyticsIntegTestCase {

    private static final String NUMERICAL_FEATURE_FIELD = "feature";
    private static final String DISCRETE_NUMERICAL_FEATURE_FIELD = "discrete-feature";
    static final String DEPENDENT_VARIABLE_FIELD = "variable";

    // It's important that the values here do not work in a way where
    // one of the feature is the average of the other features as it may
    // result in empty feature importance and we want to assert it gets
    // written out correctly.
    private static final List<Double> NUMERICAL_FEATURE_VALUES = List.of(5.0, 2.0, 3.0);
    private static final List<Long> DISCRETE_NUMERICAL_FEATURE_VALUES = List.of(50L, 20L, 30L);
    private static final List<Double> DEPENDENT_VARIABLE_VALUES = List.of(500.0, 200.0, 300.0);

    private String jobId;
    private String sourceIndex;
    private String destIndex;

    @After
    public void cleanup() {
        cleanUp();
    }

    public void testSingleNumericFeatureAndMixedTrainingAndNonTrainingRows() throws Exception {
        initialize("regression_single_numeric_feature_and_mixed_data_set");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 300, 50);

        DataFrameAnalyticsConfig config = buildAnalytics(
            jobId,
            sourceIndex,
            destIndex,
            null,
            new Regression(
                DEPENDENT_VARIABLE_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        // for debugging
        List<Map<String, Object>> badDocuments = new ArrayList<>();
        assertResponse(prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000), sourceData -> {
            int trainingDocsWithEmptyFeatureImportance = 0;
            int testDocsWithEmptyFeatureImportance = 0;
            for (SearchHit hit : sourceData.getHits()) {
                Map<String, Object> destDoc = getDestDoc(config, hit);
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

                // TODO reenable this assertion when the backend is stable
                // it seems for this case values can be as far off as 2.0

                // double featureValue = (double) destDoc.get(NUMERICAL_FEATURE_FIELD);
                // double predictionValue = (double) resultsObject.get(predictedClassField);
                // assertThat(predictionValue, closeTo(10 * featureValue, 2.0));

                assertThat(resultsObject, hasKey(predictedClassField));
                assertThat(resultsObject, hasEntry("is_training", destDoc.containsKey(DEPENDENT_VARIABLE_FIELD)));
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> importanceArray = (List<Map<String, Object>>) resultsObject.get("feature_importance");

                if (importanceArray.isEmpty()) {
                    badDocuments.add(destDoc);
                    if (Boolean.TRUE.equals(resultsObject.get("is_training"))) {
                        trainingDocsWithEmptyFeatureImportance++;
                    } else {
                        testDocsWithEmptyFeatureImportance++;
                    }
                }

                assertThat(
                    importanceArray,
                    hasItem(
                        either(Matchers.<String, Object>hasEntry("feature_name", NUMERICAL_FEATURE_FIELD)).or(
                            hasEntry("feature_name", DISCRETE_NUMERICAL_FEATURE_FIELD)
                        )
                    )
                );
            }

            // If feature importance was empty for some of the docs this assertion helps us
            // understand whether the offending docs were training or test docs.
            assertThat(
                "There were ["
                    + trainingDocsWithEmptyFeatureImportance
                    + "] training docs and ["
                    + testDocsWithEmptyFeatureImportance
                    + "] test docs with empty feature importance"
                    + " from "
                    + sourceData.getHits().getTotalHits().value()
                    + " hits.\n"
                    + badDocuments,
                trainingDocsWithEmptyFeatureImportance + testDocsWithEmptyFeatureImportance,
                equalTo(0)
            );
        });

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(
            jobId,
            "Created analytics with type [regression]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsHundred() throws Exception {
        initialize("regression_only_training_data_and_training_percent_is_100");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);
        assertResponse(prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000), sourceData -> {
            for (SearchHit hit : sourceData.getHits()) {
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

                assertThat(resultsObject.containsKey(predictedClassField), is(true));
                assertThat(resultsObject.containsKey("is_training"), is(true));
                assertThat(resultsObject.get("is_training"), is(true));
            }
        });

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);

        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(jobId);
        assertThat(stats.getDataCounts().getJobId(), equalTo(jobId));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), equalTo(350L));
        assertThat(stats.getDataCounts().getTestDocsCount(), equalTo(0L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(
            jobId,
            "Created analytics with type [regression]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testWithOnlyTrainingRowsAndTrainingPercentIsFifty() throws Exception {
        initialize("regression_only_training_data_and_training_percent_is_50");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(
            jobId,
            sourceIndex,
            destIndex,
            null,
            new Regression(DEPENDENT_VARIABLE_FIELD, BoostedTreeParams.builder().build(), null, 50.0, null, null, null, null, null)
        );
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertResponse(prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000), sourceData -> {
            int trainingRowsCount = 0;
            int nonTrainingRowsCount = 0;
            for (SearchHit hit : sourceData.getHits()) {
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

                assertThat(resultsObject.containsKey(predictedClassField), is(true));
                assertThat(resultsObject.containsKey("is_training"), is(true));
                // Let's just assert there's both training and non-training results
                if ((boolean) resultsObject.get("is_training")) {
                    trainingRowsCount++;
                } else {
                    nonTrainingRowsCount++;
                }
            }
            assertThat(trainingRowsCount, greaterThan(0));
            assertThat(nonTrainingRowsCount, greaterThan(0));
        });

        GetDataFrameAnalyticsStatsAction.Response.Stats stats = getAnalyticsStats(jobId);
        assertThat(stats.getDataCounts().getJobId(), equalTo(jobId));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), greaterThan(0L));
        assertThat(stats.getDataCounts().getTrainingDocsCount(), lessThan(350L));
        assertThat(stats.getDataCounts().getTestDocsCount(), greaterThan(0L));
        assertThat(stats.getDataCounts().getTestDocsCount(), lessThan(350L));
        assertThat(stats.getDataCounts().getSkippedDocsCount(), equalTo(0L));

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(
            jobId,
            "Created analytics with type [regression]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testStopAndRestart() throws Exception {
        initialize("regression_stop_and_restart");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 350, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        NodeAcknowledgedResponse response = startAnalytics(jobId);
        assertThat(response.getNode(), not(emptyString()));

        String phaseToWait = randomFrom("reindexing", "loading_data", "feature_selection", "fine_tuning_parameters");
        waitUntilSomeProgressHasBeenMadeForPhase(jobId, phaseToWait);
        stopAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        // Now let's start it again
        try {
            response = startAnalytics(jobId);
            assertThat(response.getNode(), not(emptyString()));
        } catch (Exception e) {
            if (e.getMessage().equals("Cannot start because the job has already finished")) {
                // That means the job had managed to complete
            } else {
                throw e;
            }
        }

        waitUntilAnalyticsIsStopped(jobId);

        assertResponse(prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000), sourceData -> {
            for (SearchHit hit : sourceData.getHits()) {
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(getDestDoc(config, hit));

                assertThat(resultsObject.containsKey(predictedClassField), is(true));
                assertThat(resultsObject.containsKey("is_training"), is(true));
                assertThat(resultsObject.get("is_training"), is(true));
            }
        });

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertAtLeastOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
    }

    public void testTwoJobsWithSameRandomizeSeedUseSameTrainingSet() throws Exception {
        String sourceIndex = "regression_two_jobs_with_same_randomize_seed_source";
        indexData(sourceIndex, 100, 0);

        String firstJobId = "regression_two_jobs_with_same_randomize_seed_1";
        String firstJobDestIndex = firstJobId + "_dest";

        BoostedTreeParams boostedTreeParams = BoostedTreeParams.builder()
            .setLambda(1.0)
            .setGamma(1.0)
            .setEta(1.0)
            .setFeatureBagFraction(1.0)
            .setMaxTrees(1)
            .build();

        DataFrameAnalyticsConfig firstJob = buildAnalytics(
            firstJobId,
            sourceIndex,
            firstJobDestIndex,
            null,
            new Regression(DEPENDENT_VARIABLE_FIELD, boostedTreeParams, null, 50.0, null, null, null, null, null)
        );
        putAnalytics(firstJob);
        startAnalytics(firstJobId);
        waitUntilAnalyticsIsStopped(firstJobId);

        String secondJobId = "regression_two_jobs_with_same_randomize_seed_2";
        String secondJobDestIndex = secondJobId + "_dest";

        long randomizeSeed = ((Regression) firstJob.getAnalysis()).getRandomizeSeed();
        DataFrameAnalyticsConfig secondJob = buildAnalytics(
            secondJobId,
            sourceIndex,
            secondJobDestIndex,
            null,
            new Regression(DEPENDENT_VARIABLE_FIELD, boostedTreeParams, null, 50.0, randomizeSeed, null, null, null, null)
        );

        putAnalytics(secondJob);
        startAnalytics(secondJobId);
        waitUntilAnalyticsIsStopped(secondJobId);

        // Now we compare they both used the same training rows
        Set<String> firstRunTrainingRowsIds = getTrainingRowsIds(firstJobDestIndex);
        Set<String> secondRunTrainingRowsIds = getTrainingRowsIds(secondJobDestIndex);

        assertThat(secondRunTrainingRowsIds, equalTo(firstRunTrainingRowsIds));
    }

    public void testDeleteExpiredData_RemovesUnusedState() throws Exception {
        initialize("regression_delete_expired_data");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 100, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        putAnalytics(config);
        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");

        // Call _delete_expired_data API and check nothing was deleted
        assertThat(deleteExpiredData().isDeleted(), is(true));
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());

        // Delete the config straight from the config index
        DeleteResponse deleteResponse = client().prepareDelete(".ml-config", DataFrameAnalyticsConfig.documentId(jobId))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(deleteResponse.status(), equalTo(RestStatus.OK));

        // Now calling the _delete_expired_data API should remove unused state
        assertThat(deleteExpiredData().isDeleted(), is(true));

        assertHitCount(prepareSearch(".ml-state*"), 0L);
    }

    public void testDependentVariableIsLong() throws Exception {
        initialize("regression_dependent_variable_is_long");
        String predictedClassField = DISCRETE_NUMERICAL_FEATURE_FIELD + "_prediction";
        indexData(sourceIndex, 100, 0);

        DataFrameAnalyticsConfig config = buildAnalytics(
            jobId,
            sourceIndex,
            destIndex,
            null,
            new Regression(DISCRETE_NUMERICAL_FEATURE_FIELD, BoostedTreeParams.builder().build(), null, null, null, null, null, null, null)
        );
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);
        assertProgressComplete(jobId);

        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
    }

    public void testWithDatastream() throws Exception {
        initialize("regression_with_datastream");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 300, 50, true);

        DataFrameAnalyticsConfig config = buildAnalytics(
            jobId,
            sourceIndex,
            destIndex,
            null,
            new Regression(
                DEPENDENT_VARIABLE_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null,
                null,
                null
            )
        );
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        assertResponse(prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000), sourceData -> {
            for (SearchHit hit : sourceData.getHits()) {
                Map<String, Object> destDoc = getDestDoc(config, hit);
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

                assertThat(resultsObject, hasKey(predictedClassField));
                assertThat(resultsObject, hasEntry("is_training", destDoc.containsKey(DEPENDENT_VARIABLE_FIELD)));
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> importanceArray = (List<Map<String, Object>>) resultsObject.get("feature_importance");

                assertThat(
                    importanceArray,
                    hasItem(
                        either(Matchers.<String, Object>hasEntry("feature_name", NUMERICAL_FEATURE_FIELD)).or(
                            hasEntry("feature_name", DISCRETE_NUMERICAL_FEATURE_FIELD)
                        )
                    )
                );
            }
        });

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(
            jobId,
            "Created analytics with type [regression]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/93228")
    public void testAliasFields() throws Exception {
        // The goal of this test is to assert alias fields are included in the analytics job.
        // We have a simple dataset with two integer fields: field_1 and field_2.
        // field_2 is double the value of field_1.
        // We also add an alias to field_1 and we exclude field_1 from the analysis forcing
        // field_1_alias to be the feature and field_2 to be the dependent variable.
        // Then we proceed to check the predictions are roughly double the feature value.
        // If alias fields are not being extracted properly the predictions will be wrong.

        initialize("regression_alias_fields");
        String predictionField = "field_2_prediction";

        String mapping = """
            {
                "properties": {
                    "field_1": {
                        "type": "integer"
                    },
                    "field_2": {
                        "type": "integer"
                    },
                    "field_1_alias": {
                        "type": "alias",
                        "path": "field_1"
                    }
                }
            }""";
        client().admin().indices().prepareCreate(sourceIndex).setMapping(mapping).get();

        int totalDocCount = 300;
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < totalDocCount; i++) {
            List<Object> source = List.of("field_1", i, "field_2", 2 * i);
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray()).opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }

        // Very infrequently this test may fail as the algorithm underestimates the
        // required number of trees for this simple problem. This failure is irrelevant
        // for non-trivial real-world problem and improving estimation of the number of trees
        // would introduce unnecessary overhead. Hence, to reduce the noise from this test we fix the seed.
        long seed = 1000L; // fix seed

        Regression regression = new Regression("field_2", BoostedTreeParams.builder().build(), null, 90.0, seed, null, null, null, null);
        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(jobId)
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex }, null, null, Collections.emptyMap()))
            .setDest(new DataFrameAnalyticsDest(destIndex, null))
            .setAnalysis(regression)
            .setAnalyzedFields(FetchSourceContext.of(true, null, new String[] { "field_1" }))
            .build();
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);

        waitUntilAnalyticsIsStopped(jobId);

        // obtain addition information for investigation of #90599
        String modelId = getModelId(jobId);
        TrainedModelMetadata modelMetadata = getModelMetadata(modelId);
        assertThat(modelMetadata.getHyperparameters().size(), greaterThan(0));
        StringBuilder hyperparameters = new StringBuilder(); // used to investigate #90599
        for (Hyperparameters hyperparameter : modelMetadata.getHyperparameters()) {
            hyperparameters.append(hyperparameter.hyperparameterName).append(": ").append(hyperparameter.value).append("\n");
        }
        TrainedModelDefinition modelDefinition = getModelDefinition(modelId);
        Ensemble ensemble = (Ensemble) modelDefinition.getTrainedModel();
        int numberTrees = ensemble.getModels().size();

        StringBuilder targetsPredictions = new StringBuilder(); // used to investigate #90599
        assertResponse(prepareSearch(sourceIndex).setSize(totalDocCount), sourceData -> {
            double predictionErrorSum = 0.0;
            for (SearchHit hit : sourceData.getHits()) {
                Map<String, Object> destDoc = getDestDoc(config, hit);
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

                assertThat(resultsObject.containsKey(predictionField), is(true));
                assertThat(resultsObject.containsKey("is_training"), is(true));

                int featureValue = (int) destDoc.get("field_1");
                double predictionValue = (double) resultsObject.get(predictionField);
                predictionErrorSum += Math.abs(predictionValue - 2 * featureValue);

                // collect the log of targets and predictions for debugging #90599
                targetsPredictions.append(2 * featureValue).append(", ").append(predictionValue).append("\n");
            }
            // We assert on the mean prediction error in order to reduce the probability
            // the test fails compared to asserting on the prediction of each individual doc.
            double meanPredictionError = predictionErrorSum / sourceData.getHits().getHits().length;
            String str = "Failure: failed for seed %d inferenceEntityId %s numberTrees %d\n";
            assertThat(
                Strings.format(str, seed, modelId, numberTrees) + targetsPredictions + hyperparameters,
                meanPredictionError,
                lessThanOrEqualTo(3.0)
            );
        });

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictionField, "double");
        assertThatAuditMessagesMatch(
            jobId,
            "Created analytics with type [regression]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testWithCustomFeatureProcessors() throws Exception {
        initialize("regression_with_custom_feature_processors");
        String predictedClassField = DEPENDENT_VARIABLE_FIELD + "_prediction";
        indexData(sourceIndex, 300, 50);

        DataFrameAnalyticsConfig config = buildAnalytics(
            jobId,
            sourceIndex,
            destIndex,
            null,
            new Regression(
                DEPENDENT_VARIABLE_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null,
                Arrays.asList(
                    new OneHotEncoding(
                        DISCRETE_NUMERICAL_FEATURE_FIELD,
                        Collections.singletonMap(DISCRETE_NUMERICAL_FEATURE_VALUES.get(0).toString(), "tenner"),
                        true
                    )
                ),
                null
            )
        );
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);

        // for debugging
        assertResponse(prepareSearch(sourceIndex).setTrackTotalHits(true).setSize(1000), sourceData -> {
            for (SearchHit hit : sourceData.getHits()) {
                Map<String, Object> destDoc = getDestDoc(config, hit);
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

                assertThat(resultsObject.containsKey(predictedClassField), is(true));
                assertThat(resultsObject.containsKey("is_training"), is(true));
                assertThat(resultsObject.get("is_training"), is(destDoc.containsKey(DEPENDENT_VARIABLE_FIELD)));
            }
        });

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(
            jobId,
            "Created analytics with type [regression]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
        GetTrainedModelsAction.Response response = client().execute(
            GetTrainedModelsAction.INSTANCE,
            new GetTrainedModelsAction.Request(jobId + "*", Collections.emptyList(), Collections.singleton("definition"))
        ).actionGet();
        assertThat(response.getResources().results().size(), equalTo(1));
        TrainedModelConfig modelConfig = response.getResources().results().get(0);
        modelConfig.ensureParsedDefinition(xContentRegistry());
        assertThat(modelConfig.getModelDefinition().getPreProcessors().size(), greaterThan(0));
        for (int i = 0; i < modelConfig.getModelDefinition().getPreProcessors().size(); i++) {
            PreProcessor preProcessor = modelConfig.getModelDefinition().getPreProcessors().get(i);
            assertThat(preProcessor.isCustom(), equalTo(i == 0));
        }
    }

    public void testWithSearchRuntimeMappings() throws Exception {
        initialize("regression_with_search_runtime_mappings");
        indexData(sourceIndex, 300, 50);

        String numericRuntimeField = NUMERICAL_FEATURE_FIELD + "_runtime";
        String dependentVariableRuntimeField = DEPENDENT_VARIABLE_FIELD + "_runtime";

        String predictedClassField = dependentVariableRuntimeField + "_prediction";

        Map<String, Object> numericRuntimeFieldMapping = new HashMap<>();
        numericRuntimeFieldMapping.put("type", "double");
        numericRuntimeFieldMapping.put("script", "emit(doc['" + NUMERICAL_FEATURE_FIELD + "'].value)");
        Map<String, Object> dependentVariableRuntimeFieldMapping = new HashMap<>();
        dependentVariableRuntimeFieldMapping.put("type", "double");
        dependentVariableRuntimeFieldMapping.put(
            "script",
            "if (doc['" + DEPENDENT_VARIABLE_FIELD + "'].size() > 0) { emit(doc['" + DEPENDENT_VARIABLE_FIELD + "'].value); }"
        );
        Map<String, Object> runtimeFields = new HashMap<>();
        runtimeFields.put(numericRuntimeField, numericRuntimeFieldMapping);
        runtimeFields.put(dependentVariableRuntimeField, dependentVariableRuntimeFieldMapping);

        DataFrameAnalyticsConfig config = new DataFrameAnalyticsConfig.Builder().setId(jobId)
            .setSource(new DataFrameAnalyticsSource(new String[] { sourceIndex }, null, null, runtimeFields))
            .setDest(new DataFrameAnalyticsDest(destIndex, null))
            .setAnalyzedFields(FetchSourceContext.of(true, new String[] { numericRuntimeField, dependentVariableRuntimeField }, null))
            .setAnalysis(
                new Regression(
                    dependentVariableRuntimeField,
                    BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
                )
            )
            .build();
        putAnalytics(config);

        assertIsStopped(jobId);
        assertProgressIsZero(jobId);

        startAnalytics(jobId);
        waitUntilAnalyticsIsStopped(jobId);
        assertResponse(prepareSearch(destIndex).setTrackTotalHits(true).setSize(1000), destData -> {
            for (SearchHit hit : destData.getHits()) {
                Map<String, Object> destDoc = hit.getSourceAsMap();
                Map<String, Object> resultsObject = getMlResultsObjectFromDestDoc(destDoc);

                assertThat(resultsObject.containsKey(predictedClassField), is(true));
                assertThat(resultsObject.containsKey("is_training"), is(true));
                assertThat(resultsObject.get("is_training"), is(destDoc.containsKey(DEPENDENT_VARIABLE_FIELD)));
                @SuppressWarnings("unchecked")
                List<Map<String, Object>> importanceArray = (List<Map<String, Object>>) resultsObject.get("feature_importance");
                assertThat(importanceArray, hasSize(1));
                assertThat(importanceArray.get(0), hasEntry("feature_name", numericRuntimeField));
            }
        });

        assertProgressComplete(jobId);
        assertStoredProgressHits(jobId, 1);
        assertModelStatePersisted(stateDocId());
        assertExactlyOneInferenceModelPersisted(jobId);
        assertMlResultsFieldMappings(destIndex, predictedClassField, "double");
        assertThatAuditMessagesMatch(
            jobId,
            "Created analytics with type [regression]",
            "Estimated memory usage [",
            "Starting analytics on node",
            "Started analytics",
            "Creating destination index [" + destIndex + "]",
            "Started reindexing to destination index [" + destIndex + "]",
            "Finished reindexing to destination index [" + destIndex + "]",
            "Started loading data",
            "Started analyzing",
            "Started writing results",
            "Finished analysis"
        );
    }

    public void testPreview() throws Exception {
        initialize("preview_analytics");
        indexData(sourceIndex, 300, 50);

        DataFrameAnalyticsConfig config = buildAnalytics(jobId, sourceIndex, destIndex, null, new Regression(DEPENDENT_VARIABLE_FIELD));
        putAnalytics(config);
        List<Map<String, Object>> preview = previewDataFrame(jobId).getFeatureValues();
        for (Map<String, Object> feature : preview) {
            assertThat(feature.keySet(), hasItems(NUMERICAL_FEATURE_FIELD, DISCRETE_NUMERICAL_FEATURE_FIELD, DEPENDENT_VARIABLE_FIELD));
        }
    }

    public void testPreviewWithProcessors() throws Exception {
        initialize("processed_preview_analytics");
        indexData(sourceIndex, 300, 50);

        DataFrameAnalyticsConfig config = buildAnalytics(
            jobId,
            sourceIndex,
            destIndex,
            null,
            new Regression(
                DEPENDENT_VARIABLE_FIELD,
                BoostedTreeParams.builder().setNumTopFeatureImportanceValues(1).build(),
                null,
                null,
                null,
                null,
                null,
                Arrays.asList(
                    new OneHotEncoding(
                        DISCRETE_NUMERICAL_FEATURE_FIELD,
                        Collections.singletonMap(DISCRETE_NUMERICAL_FEATURE_VALUES.get(0).toString(), "tenner"),
                        true
                    )
                ),
                null
            )
        );
        putAnalytics(config);
        List<Map<String, Object>> preview = previewDataFrame(jobId).getFeatureValues();
        for (Map<String, Object> feature : preview) {
            assertThat(feature.keySet(), hasItems(NUMERICAL_FEATURE_FIELD, "tenner", DEPENDENT_VARIABLE_FIELD));
            assertThat(feature, not(hasKey(DISCRETE_NUMERICAL_FEATURE_VALUES)));
        }
    }

    private void initialize(String jobId) {
        this.jobId = jobId;
        this.sourceIndex = jobId + "_source_index";
        this.destIndex = sourceIndex + "_results";
    }

    static void indexData(String sourceIndex, int numTrainingRows, int numNonTrainingRows) {
        indexData(sourceIndex, numTrainingRows, numNonTrainingRows, false);
    }

    static void indexData(String sourceIndex, int numTrainingRows, int numNonTrainingRows, boolean dataStream) {
        String mapping = Strings.format("""
            {
              "properties": {
                "@timestamp": {
                  "type": "date"
                },
                "%s": {
                  "type": "double"
                },
                "%s": {
                  "type": "unsigned_long"
                },
                "%s": {
                  "type": "double"
                }
              }
            }""", NUMERICAL_FEATURE_FIELD, DISCRETE_NUMERICAL_FEATURE_FIELD, DEPENDENT_VARIABLE_FIELD);
        if (dataStream) {
            try {
                createDataStreamAndTemplate(sourceIndex, mapping);
            } catch (IOException ex) {
                throw new ElasticsearchException(ex);
            }
        } else {
            client().admin().indices().prepareCreate(sourceIndex).setMapping(mapping).get();
        }

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < numTrainingRows; i++) {
            List<Object> source = List.of(
                NUMERICAL_FEATURE_FIELD,
                NUMERICAL_FEATURE_VALUES.get(i % NUMERICAL_FEATURE_VALUES.size()),
                DISCRETE_NUMERICAL_FEATURE_FIELD,
                DISCRETE_NUMERICAL_FEATURE_VALUES.get(i % DISCRETE_NUMERICAL_FEATURE_VALUES.size()),
                DEPENDENT_VARIABLE_FIELD,
                DEPENDENT_VARIABLE_VALUES.get(i % DEPENDENT_VARIABLE_VALUES.size()),
                "@timestamp",
                Instant.now().toEpochMilli()
            );
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray()).opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        for (int i = numTrainingRows; i < numTrainingRows + numNonTrainingRows; i++) {
            List<Object> source = List.of(
                NUMERICAL_FEATURE_FIELD,
                NUMERICAL_FEATURE_VALUES.get(i % NUMERICAL_FEATURE_VALUES.size()),
                DISCRETE_NUMERICAL_FEATURE_FIELD,
                DISCRETE_NUMERICAL_FEATURE_VALUES.get(i % DISCRETE_NUMERICAL_FEATURE_VALUES.size()),
                "@timestamp",
                Instant.now().toEpochMilli()
            );
            IndexRequest indexRequest = new IndexRequest(sourceIndex).source(source.toArray()).opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.get();
        if (bulkResponse.hasFailures()) {
            fail("Failed to index data: " + bulkResponse.buildFailureMessage());
        }
    }

    private static Map<String, Object> getDestDoc(DataFrameAnalyticsConfig config, SearchHit hit) {
        GetResponse destDocGetResponse = client().prepareGet().setIndex(config.getDest().getIndex()).setId(hit.getId()).get();
        assertThat(destDocGetResponse.isExists(), is(true));
        Map<String, Object> sourceDoc = hit.getSourceAsMap();
        Map<String, Object> destDoc = destDocGetResponse.getSource();
        for (String field : sourceDoc.keySet()) {
            assertThat(destDoc.containsKey(field), is(true));
            assertThat(destDoc.get(field), equalTo(sourceDoc.get(field)));
        }
        return destDoc;
    }

    private static Map<String, Object> getMlResultsObjectFromDestDoc(Map<String, Object> destDoc) {
        return getFieldValue(destDoc, "ml");
    }

    protected String stateDocId() {
        return jobId + "_regression_state#1";
    }

    @Override
    boolean supportsInference() {
        return true;
    }
}
