/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.persistence;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.template.TemplateUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Methods for handling index naming related functions
 */
public final class AnomalyDetectorsIndex {

    public static final int CONFIG_INDEX_MAX_RESULTS_WINDOW = 10_000;

    private static final String RESULTS_MAPPINGS_VERSION_VARIABLE = "xpack.ml.version";
    private static final String RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/anomalydetection/";

    // Visible for testing
    static final Comparator<String> STATE_INDEX_NAME_COMPARATOR = new Comparator<>() {

        private final Predicate<String> HAS_SIX_DIGIT_SUFFIX = Pattern.compile("\\d{6}").asMatchPredicate();

        @Override
        public int compare(String index1, String index2) {
            String[] index1Parts = index1.split("-");
            String index1Suffix = index1Parts[index1Parts.length - 1];
            boolean index1HasSixDigitsSuffix = HAS_SIX_DIGIT_SUFFIX.test(index1Suffix);
            String[] index2Parts = index2.split("-");
            String index2Suffix = index2Parts[index2Parts.length - 1];
            boolean index2HasSixDigitsSuffix = HAS_SIX_DIGIT_SUFFIX.test(index2Suffix);
            if (index1HasSixDigitsSuffix && index2HasSixDigitsSuffix) {
                return index1Suffix.compareTo(index2Suffix);
            } else if (index1HasSixDigitsSuffix != index2HasSixDigitsSuffix) {
                return Boolean.compare(index1HasSixDigitsSuffix, index2HasSixDigitsSuffix);
            } else {
                return index1.compareTo(index2);
            }
        }
    };

    private AnomalyDetectorsIndex() {
    }

    public static String jobResultsIndexPrefix() {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX;
    }

    /**
     * The name of the alias pointing to the indices where the job's results are stored
     * @param jobId Job Id
     * @return The read alias
     */
    public static String jobResultsAliasedName(String jobId) {
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + jobId;
    }

    /**
     * The name of the alias pointing to the write index for a job
     * @param jobId Job Id
     * @return The write alias
     */
    public static String resultsWriteAlias(String jobId) {
        // ".write" rather than simply "write" to avoid the danger of clashing
        // with the read alias of a job whose name begins with "write-"
        return AnomalyDetectorsIndexFields.RESULTS_INDEX_PREFIX + ".write-" + jobId;
    }

    /**
     * The name of the alias pointing to the appropriate index for writing job state
     * @return The write alias name
     */
    public static String jobStateIndexWriteAlias() {
        return AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-write";
    }

    /**
     * The name pattern to capture all .ml-state prefixed indices
     * @return The .ml-state index pattern
     */
    public static String jobStateIndexPattern() {
        return AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "*";
    }

    /**
     * The name of the index where job and datafeed configuration
     * is stored
     * @return The index name
     */
    public static String configIndexName() {
        return AnomalyDetectorsIndexFields.CONFIG_INDEX;
    }

    /**
     * Creates the .ml-state-000001 index (if necessary)
     * Creates the .ml-state-write alias for the .ml-state-000001 index (if necessary)
     */
    public static void createStateIndexAndAliasIfNecessary(Client client, ClusterState state, final ActionListener<Boolean> finalListener) {

        if (state.getMetaData().getAliasAndIndexLookup().containsKey(jobStateIndexWriteAlias())) {
            finalListener.onResponse(false);
            return;
        }

        final ActionListener<String> createAliasListener = ActionListener.wrap(
            concreteIndexName -> {
                final IndicesAliasesRequest request = client.admin()
                    .indices()
                    .prepareAliases()
                    .addAlias(concreteIndexName, jobStateIndexWriteAlias())
                    .request();
                executeAsyncWithOrigin(client.threadPool().getThreadContext(),
                    ML_ORIGIN,
                    request,
                    ActionListener.<AcknowledgedResponse>wrap(
                        resp -> finalListener.onResponse(resp.isAcknowledged()),
                        finalListener::onFailure),
                    client.admin().indices()::aliases);
            },
            finalListener::onFailure
        );

        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver();
        String[] stateIndices = indexNameExpressionResolver.concreteIndexNames(state,
            IndicesOptions.lenientExpandOpen(),
            jobStateIndexPattern());
        if (stateIndices.length > 0) {
            String latestStateIndex = Arrays.stream(stateIndices).max(STATE_INDEX_NAME_COMPARATOR).get();
            createAliasListener.onResponse(latestStateIndex);
        } else {
            // The initial index name must be suitable for rollover functionality.
            String initialJobStateIndex = AnomalyDetectorsIndexFields.STATE_INDEX_PREFIX + "-000001";
            CreateIndexRequest createIndexRequest = client.admin()
                .indices()
                .prepareCreate(initialJobStateIndex)
                .addAlias(new Alias(jobStateIndexWriteAlias()))
                .request();
            executeAsyncWithOrigin(client.threadPool().getThreadContext(),
                ML_ORIGIN,
                createIndexRequest,
                ActionListener.<CreateIndexResponse>wrap(
                    createIndexResponse -> finalListener.onResponse(true),
                    createIndexFailure -> {
                        // If it was created between our last check, and this request being handled, we should add the alias
                        // Adding an alias that already exists is idempotent. So, no need to double check if the alias exists
                        // as well.
                        if (ExceptionsHelper.unwrapCause(createIndexFailure) instanceof ResourceAlreadyExistsException) {
                            createAliasListener.onResponse(initialJobStateIndex);
                        } else {
                            finalListener.onFailure(createIndexFailure);
                        }
                    }),
                client.admin().indices()::create);
        }
    }

    public static String resultsMapping() {
        return TemplateUtils.loadTemplate(RESOURCE_PATH + "results_index_mappings.json",
            Version.CURRENT.toString(), RESULTS_MAPPINGS_VERSION_VARIABLE);
    }
}
