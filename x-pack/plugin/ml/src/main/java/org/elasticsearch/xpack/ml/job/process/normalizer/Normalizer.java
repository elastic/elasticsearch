/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.ml.job.process.normalizer.output.NormalizerResultHandler;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Normalizes probabilities to scores in the range 0-100.
 * <br>
 * Creates and initialises the normalizer process, pipes the probabilities
 * through them and adds the normalized values to the records/buckets.
 * <br>
 * Relies on the C++ normalizer process returning an answer for every input
 * and in exactly the same order as the inputs.
 */
public class Normalizer {
    private static final Logger LOGGER = LogManager.getLogger(Normalizer.class);

    private final String jobId;
    private final NormalizerProcessFactory processFactory;
    private final ExecutorService executorService;

    public Normalizer(String jobId, NormalizerProcessFactory processFactory, ExecutorService executorService) {
        this.jobId = jobId;
        this.processFactory = processFactory;
        this.executorService = executorService;
    }

    /**
     * Launches a normalization process seeded with the quantiles state provided
     * and normalizes the given results.
     *
     * @param bucketSpan                If <code>null</code> the default is used
     * @param results                   Will be updated with the normalized results
     * @param quantilesState            The state to be used to seed the system change
     *                                  normalizer
     */
    public void normalize(Integer bucketSpan,
                          List<? extends Normalizable> results, String quantilesState) {
        NormalizerProcess process = processFactory.createNormalizerProcess(jobId, quantilesState, bucketSpan,
                 executorService);
        NormalizerResultHandler resultsHandler = process.createNormalizedResultsHandler();
        Future<?> resultsHandlerFuture = executorService.submit(() -> {
            try {
                resultsHandler.process();
            } catch (IOException e) {
                LOGGER.error(new ParameterizedMessage("[{}] Error reading normalizer results", new Object[] { jobId }), e);
            }
        });

        try {
            process.writeRecord(new String[] {
                    NormalizerResult.LEVEL_FIELD.getPreferredName(),
                    NormalizerResult.PARTITION_FIELD_NAME_FIELD.getPreferredName(),
                    NormalizerResult.PARTITION_FIELD_VALUE_FIELD.getPreferredName(),
                    NormalizerResult.PERSON_FIELD_NAME_FIELD.getPreferredName(),
                    NormalizerResult.PERSON_FIELD_VALUE_FIELD.getPreferredName(),
                    NormalizerResult.FUNCTION_NAME_FIELD.getPreferredName(),
                    NormalizerResult.VALUE_FIELD_NAME_FIELD.getPreferredName(),
                    NormalizerResult.PROBABILITY_FIELD.getPreferredName(),
                    NormalizerResult.NORMALIZED_SCORE_FIELD.getPreferredName()
            });

            for (Normalizable result : results) {
                writeNormalizableAndChildrenRecursively(result, process);
            }
        } catch (IOException e) {
            LOGGER.error("[" + jobId + "] Error writing to the normalizer", e);
        } finally {
            try {
                process.close();
            } catch (IOException e) {
                LOGGER.error("[" + jobId + "] Error closing normalizer", e);
            }
        }

        // Wait for the results handler to finish
        try {
            resultsHandlerFuture.get();
            mergeNormalizedScoresIntoResults(resultsHandler.getNormalizedResults(), results);
        } catch (ExecutionException e) {
            LOGGER.error(new ParameterizedMessage("[{}] Error processing normalizer results", new Object[] { jobId }), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void writeNormalizableAndChildrenRecursively(Normalizable normalizable,
                                                                NormalizerProcess process) throws IOException {
        if (normalizable.isContainerOnly() == false) {
            process.writeRecord(new String[] {
                    normalizable.getLevel().asString(),
                    Strings.coalesceToEmpty(normalizable.getPartitionFieldName()),
                    Strings.coalesceToEmpty(normalizable.getPartitionFieldValue()),
                    Strings.coalesceToEmpty(normalizable.getPersonFieldName()),
                    Strings.coalesceToEmpty(normalizable.getPersonFieldValue()),
                    Strings.coalesceToEmpty(normalizable.getFunctionName()),
                    Strings.coalesceToEmpty(normalizable.getValueFieldName()),
                    Double.toString(normalizable.getProbability()),
                    Double.toString(normalizable.getNormalizedScore())
            });
        }
        for (Normalizable child : normalizable.getChildren()) {
            writeNormalizableAndChildrenRecursively(child, process);
        }
    }

    /**
     * Updates the normalized scores on the results.
     */
    private void mergeNormalizedScoresIntoResults(List<NormalizerResult> normalizedScores,
                                                  List<? extends Normalizable> results) {
        Iterator<NormalizerResult> scoresIter = normalizedScores.iterator();
        for (Normalizable result : results) {
            mergeRecursively(scoresIter, null, false, result);
        }
        if (scoresIter.hasNext()) {
            LOGGER.error("[{}] Unused normalized scores remain after updating all results: {} for {}",
                    jobId, normalizedScores.size(), results.size());
        }
    }

    /**
     * Recursively merges the scores returned by the normalization process into the results
     *
     * @param scoresIter         an Iterator of the scores returned by the normalization process
     * @param parent             the parent result
     * @param parentHadBigChange whether the parent had a big change
     * @param result             the result to be updated
     * @return the effective normalized score of the given result
     */
    private double mergeRecursively(Iterator<NormalizerResult> scoresIter, Normalizable parent,
                                    boolean parentHadBigChange, Normalizable result) {
        boolean hasBigChange = false;
        if (result.isContainerOnly() == false) {
            if (scoresIter.hasNext() == false) {
                String msg = "Error iterating normalized results";
                LOGGER.error("[{}] {}", jobId, msg);
                throw new ElasticsearchException(msg);
            }

            result.resetBigChangeFlag();
            if (parent != null && parentHadBigChange) {
                result.setParentScore(parent.getNormalizedScore());
                result.raiseBigChangeFlag();
            }

            double normalizedScore = scoresIter.next().getNormalizedScore();
            hasBigChange = isBigUpdate(result.getNormalizedScore(), normalizedScore);
            if (hasBigChange) {
                result.setNormalizedScore(normalizedScore);
                result.raiseBigChangeFlag();
                if (parent != null) {
                    parent.raiseBigChangeFlag();
                }
            }
        }

        for (Normalizable.ChildType childrenType : result.getChildrenTypes()) {
            List<Normalizable> children = result.getChildren(childrenType);
            if (children.isEmpty() == false) {
                double maxChildrenScore = 0.0;
                for (Normalizable child : children) {
                    maxChildrenScore = Math.max(
                            mergeRecursively(scoresIter, result, hasBigChange, child),
                            maxChildrenScore);
                }
                hasBigChange |= result.setMaxChildrenScore(childrenType, maxChildrenScore);
            }
        }
        return result.getNormalizedScore();
    }

    /**
     * Encapsulate the logic for deciding whether a change to a normalized score
     * is "big".
     * <p>
     * Current logic is that a change is considered big if any of the following criteria are met:
     * <ul>
     * <li>the change  would result in a change of colour in the UI
     * (e.g. severity would be changed from WARNING to MINOR)</li>
     * <li>the change is at least 1.5</li>
     * <li>the change in values is greater than 67% of the higher of the two values.</li>
     * </ul>
     * These values have been chosen through a process of experimentation, in particular it was desired to reduce
     * the number of updates written to the results index due to renormalization events for performance reasons
     * while not changing the normalized scores greatly
     *
     * @param oldVal The old value of the normalized score
     * @param newVal The new value of the normalized score
     * @return true if the update is considered "big"
     */
    private static boolean isBigUpdate(double oldVal, double newVal) {
        if ((int) (oldVal / 25.0) != (int) (newVal / 25.0)) {
            return true;
        }
        if (Math.abs(oldVal - newVal) >= 1.5) {
            return true;
        }
        if (oldVal > newVal) {
            if (oldVal * 0.33 > newVal) {
                return true;
            }
        } else {
            if (newVal * 0.33 > oldVal) {
                return true;
            }
        }

        return false;
    }
}
