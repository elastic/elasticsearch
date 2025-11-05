/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.ml.autoscaling.AbstractNodeAvailabilityZoneMapper;

/**
 * Extension interface for customizing Machine Learning plugin behavior.
 * <p>
 * This interface allows external implementations to configure and control various aspects
 * of the Machine Learning plugin, including feature enablement, lifecycle management,
 * and infrastructure configuration.
 * </p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Implementing a custom ML extension
 * public class CustomMlExtension implements MachineLearningExtension {
 *
 *     {@literal @}Override
 *     public void configure(Settings settings) {
 *         // Custom configuration logic
 *     }
 *
 *     {@literal @}Override
 *     public boolean useIlm() {
 *         return true; // Enable Index Lifecycle Management
 *     }
 *
 *     {@literal @}Override
 *     public boolean isAnomalyDetectionEnabled() {
 *         return true; // Enable anomaly detection feature
 *     }
 *
 *     {@literal @}Override
 *     public boolean isNlpEnabled() {
 *         return true; // Enable NLP capabilities
 *     }
 * }
 * }</pre>
 */
public interface MachineLearningExtension {

    /**
     * Configures the extension with the provided settings.
     * <p>
     * This method is called during initialization to allow the extension to
     * configure itself based on cluster settings.
     * </p>
     *
     * @param settings the cluster settings
     */
    default void configure(Settings settings) {}

    /**
     * Indicates whether to use Index Lifecycle Management (ILM) for ML indices.
     * <p>
     * When enabled, ML indices will be managed by ILM policies for automatic
     * lifecycle management including rollover, retention, and deletion.
     * </p>
     *
     * @return true if ILM should be used for ML indices, false otherwise
     */
    boolean useIlm();

    /**
     * Indicates whether to include node information in ML audit messages.
     * <p>
     * When enabled, audit messages will include the names of nodes where
     * ML tasks are assigned or running.
     * </p>
     *
     * @return true if node information should be included in audit messages, false otherwise
     */
    boolean includeNodeInfo();

    /**
     * Indicates whether anomaly detection features are enabled.
     * <p>
     * Controls the availability of anomaly detection jobs and related functionality.
     * </p>
     *
     * @return true if anomaly detection is enabled, false otherwise
     */
    boolean isAnomalyDetectionEnabled();

    /**
     * Indicates whether data frame analytics features are enabled.
     * <p>
     * Controls the availability of data frame analytics jobs including regression,
     * classification, and outlier detection.
     * </p>
     *
     * @return true if data frame analytics is enabled, false otherwise
     */
    boolean isDataFrameAnalyticsEnabled();

    /**
     * Indicates whether Natural Language Processing (NLP) features are enabled.
     * <p>
     * Controls the availability of NLP models and inference capabilities.
     * </p>
     *
     * @return true if NLP is enabled, false otherwise
     */
    boolean isNlpEnabled();

    /**
     * Indicates whether the inference process cache should be disabled.
     * <p>
     * When true, the inference process cache will not be used, which may impact
     * performance but can be useful in certain deployment scenarios.
     * </p>
     *
     * @return true if the inference process cache should be disabled, false otherwise
     */
    default boolean disableInferenceProcessCache() {
        return false;
    }

    /**
     * Returns the list of allowed settings for analytics destination indices.
     * <p>
     * These settings can be specified when creating destination indices for
     * data frame analytics jobs.
     * </p>
     *
     * @return an array of allowed setting names
     */
    String[] getAnalyticsDestIndexAllowedSettings();

    /**
     * Creates a node availability zone mapper for the given settings.
     * <p>
     * The availability zone mapper is used for autoscaling and determining
     * node placement across availability zones.
     * </p>
     *
     * @param settings the cluster settings
     * @param clusterSettings the cluster settings service
     * @return a configured availability zone mapper
     */
    AbstractNodeAvailabilityZoneMapper getNodeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings);
}
