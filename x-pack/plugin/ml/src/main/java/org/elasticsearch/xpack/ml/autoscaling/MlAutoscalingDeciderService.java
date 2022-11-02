/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.time.Instant;
import java.util.List;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

public class MlAutoscalingDeciderService implements AutoscalingDeciderService, LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(MlAutoscalingDeciderService.class);

    public static final String NAME = "ml";
    public static final Setting<Integer> NUM_ANOMALY_JOBS_IN_QUEUE = Setting.intSetting("num_anomaly_jobs_in_queue", 0, 0);
    public static final Setting<Integer> NUM_ANALYTICS_JOBS_IN_QUEUE = Setting.intSetting("num_analytics_jobs_in_queue", 0, 0);
    public static final Setting<TimeValue> DOWN_SCALE_DELAY = Setting.timeSetting("down_scale_delay", TimeValue.timeValueHours(1));

    private final ScaleTimer scaleTimer;
    private final MlMemoryAutoscalingDecider memoryDecider;
    private final MlProcessorAutoscalingDecider processorDecider;

    private volatile boolean isMaster;

    public MlAutoscalingDeciderService(
        MlMemoryTracker memoryTracker,
        Settings settings,
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper,
        ClusterService clusterService
    ) {
        this(new NodeLoadDetector(memoryTracker), settings, nodeAvailabilityZoneMapper, clusterService, System::currentTimeMillis);
    }

    MlAutoscalingDeciderService(
        NodeLoadDetector nodeLoadDetector,
        Settings settings,
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper,
        ClusterService clusterService,
        LongSupplier timeSupplier
    ) {
        this.scaleTimer = new ScaleTimer(timeSupplier);
        this.memoryDecider = new MlMemoryAutoscalingDecider(
            settings,
            clusterService,
            nodeAvailabilityZoneMapper,
            nodeLoadDetector,
            scaleTimer
        );
        this.processorDecider = new MlProcessorAutoscalingDecider(scaleTimer);
        clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    public void onMaster() {
        isMaster = true;
    }

    @Override
    public void offMaster() {
        isMaster = false;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        if (isMaster == false) {
            throw new IllegalArgumentException("request for scaling information is only allowed on the master node");
        }
        logger.debug("request to scale received");
        scaleTimer.markScale();

        final ClusterState clusterState = context.state();
        final MlAutoscalingContext mlContext = new MlAutoscalingContext(clusterState);
        final NativeMemoryCapacity currentNativeMemoryCapacity = memoryDecider.currentScale(mlContext.mlNodes);
        final MlMemoryAutoscalingCapacity currentMemoryCapacity = memoryDecider.capacityFromNativeMemory(currentNativeMemoryCapacity);
        final MlProcessorAutoscalingCapacity currentProcessorCapacity = processorDecider.computeCurrentCapacity(mlContext.mlNodes);

        final MlScalingReason.Builder reasonBuilder = MlScalingReason.builder(mlContext)
            .setCurrentMlCapacity(
                new AutoscalingCapacity(
                    new AutoscalingCapacity.AutoscalingResources(
                        null,
                        currentMemoryCapacity.tierSize(),
                        currentProcessorCapacity.tierProcessors()
                    ),
                    new AutoscalingCapacity.AutoscalingResources(
                        null,
                        currentMemoryCapacity.nodeSize(),
                        currentProcessorCapacity.nodeProcessors()
                    )
                )
            )
            .setPassedConfiguration(configuration);

        // We don't need to check anything as there are no tasks
        if (mlContext.isEmpty()) {
            // This is a quick path to downscale.
            // simply return `0` for scale down if delay is satisfied
            return downscaleToZero(configuration, context, currentNativeMemoryCapacity, reasonBuilder);
        }

        MlMemoryAutoscalingCapacity memoryCapacity = memoryDecider.scale(configuration, context, mlContext);
        if (memoryCapacity.isUndetermined()) {
            // If we cannot determine memory capacity we shouldn't make any autoscaling decision
            // as it could lead to undesired capacity. For example, it could be that the processor decider decides
            // to scale down the cluster but as soon as we can determine memory requirements again we need to scale
            // back up.
            return new AutoscalingDeciderResult(
                null,
                reasonBuilder.setSimpleReason(format("[memory_decider] %s", memoryCapacity.reason())).build()
            );
        }
        MlProcessorAutoscalingCapacity processorCapacity = processorDecider.scale(configuration, context, mlContext);
        reasonBuilder.setSimpleReason(
            format("[memory_decider] %s; [processor_decider] %s", memoryCapacity.reason(), processorCapacity.reason())
        );

        return new AutoscalingDeciderResult(
            new AutoscalingCapacity(
                new AutoscalingCapacity.AutoscalingResources(null, memoryCapacity.tierSize(), processorCapacity.tierProcessors()),
                new AutoscalingCapacity.AutoscalingResources(null, memoryCapacity.nodeSize(), processorCapacity.nodeProcessors())
            ),
            reasonBuilder.build()
        );
    }

    private AutoscalingDeciderResult downscaleToZero(
        Settings configuration,
        AutoscalingDeciderContext context,
        NativeMemoryCapacity currentScale,
        MlScalingReason.Builder reasonBuilder
    ) {
        // We might be in a need zero, have zero situation, in which case it's nicer to pass a "no change" explanation
        if (currentScale.getTierMlNativeMemoryRequirementExcludingOverhead() == 0
            && currentScale.getNodeMlNativeMemoryRequirementExcludingOverhead() == 0) {
            return new AutoscalingDeciderResult(
                context.currentCapacity(),
                reasonBuilder.setSimpleReason("Passing currently perceived capacity as no scaling changes are necessary").build()
            );
        }
        long msLeftToScale = scaleTimer.markDownScaleAndGetMillisLeftFromDelay(configuration);
        if (msLeftToScale > 0) {
            return new AutoscalingDeciderResult(
                context.currentCapacity(),
                reasonBuilder.setSimpleReason(
                    format(
                        "Passing currently perceived capacity as down scale delay has not been satisfied; configured delay [%s] "
                            + "last detected scale down event [%s]. Will request scale down in approximately [%s]",
                        DOWN_SCALE_DELAY.get(configuration).getStringRep(),
                        XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(scaleTimer.downScaleDetectedMillis())),
                        TimeValue.timeValueMillis(msLeftToScale).getStringRep()
                    )
                ).build()
            );
        }
        return new AutoscalingDeciderResult(
            AutoscalingCapacity.ZERO,
            reasonBuilder.setRequiredCapacity(AutoscalingCapacity.ZERO)
                .setSimpleReason("Requesting scale down as tier and/or node size could be smaller")
                .build()
        );
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(NUM_ANALYTICS_JOBS_IN_QUEUE, NUM_ANOMALY_JOBS_IN_QUEUE, DOWN_SCALE_DELAY);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.ML_ROLE);
    }
}
