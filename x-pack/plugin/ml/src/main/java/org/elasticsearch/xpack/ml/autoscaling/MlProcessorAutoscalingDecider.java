/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Processors;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.core.ml.inference.assignment.Priority;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.utils.MlProcessors;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static java.time.Instant.ofEpochMilli;
import static org.elasticsearch.common.xcontent.XContentElasticsearchExtension.DEFAULT_FORMATTER;
import static org.elasticsearch.core.Strings.format;

class MlProcessorAutoscalingDecider {

    private static final Logger logger = LogManager.getLogger(MlProcessorAutoscalingDecider.class);

    private final ScaleTimer scaleTimer;

    MlProcessorAutoscalingDecider(ScaleTimer scaleTimer) {
        this.scaleTimer = Objects.requireNonNull(scaleTimer);
    }

    public MlProcessorAutoscalingCapacity scale(Settings configuration, AutoscalingDeciderContext context, MlAutoscalingContext mlContext) {
        TrainedModelAssignmentMetadata trainedModelAssignmentMetadata = TrainedModelAssignmentMetadata.fromState(context.state());

        if (hasUnsatisfiedDeployments(trainedModelAssignmentMetadata, mlContext.mlNodes)) {
            logger.debug(() -> "Computing required capacity as there are partially allocated deployments");
            scaleTimer.resetScaleDownCoolDown();
            return computeRequiredCapacity(trainedModelAssignmentMetadata).setReason(
                "requesting scale up as there are unsatisfied deployments"
            ).build();
        }

        final MlProcessorAutoscalingCapacity currentCapacity = computeCurrentCapacity(mlContext.mlNodes);

        final MlProcessorAutoscalingCapacity requiredCapacity = computeRequiredCapacity(trainedModelAssignmentMetadata).build();

        if (requiredCapacity.tierProcessors().roundUp() == currentCapacity.tierProcessors().roundUp()) {
            return MlProcessorAutoscalingCapacity.builder(currentCapacity.nodeProcessors(), currentCapacity.tierProcessors())
                .setReason("passing currently perceived capacity as it is fully used")
                .build();
        }

        if (MlMemoryAutoscalingDecider.modelAssignmentsRequireMoreThanHalfCpu(
            trainedModelAssignmentMetadata.allAssignments().values(),
            mlContext.mlNodes
        )) {
            return MlProcessorAutoscalingCapacity.builder(currentCapacity.nodeProcessors(), currentCapacity.tierProcessors())
                .setReason("not scaling down as model assignments require more than half of the ML tier's allocated processors")
                .build();
        }

        long msLeftToScale = scaleTimer.markDownScaleAndGetMillisLeftFromDelay(configuration);
        if (msLeftToScale <= 0) {
            return MlProcessorAutoscalingCapacity.builder(requiredCapacity.nodeProcessors(), requiredCapacity.tierProcessors())
                .setReason("requesting scale down as tier and/or node size could be smaller")
                .build();
        }

        TimeValue downScaleDelay = MlAutoscalingDeciderService.DOWN_SCALE_DELAY.get(configuration);
        logger.debug(
            () -> format(
                "not scaling down as the current scale down delay [%s] is not satisfied."
                    + " The last time scale down was detected [%s]. Calculated scaled down capacity [%s] ",
                downScaleDelay.getStringRep(),
                DEFAULT_FORMATTER.format(ofEpochMilli(scaleTimer.downScaleDetectedMillis())),
                requiredCapacity
            )
        );
        return MlProcessorAutoscalingCapacity.builder(currentCapacity.nodeProcessors(), currentCapacity.tierProcessors())
            .setReason(
                String.format(
                    Locale.ROOT,
                    "Passing currently perceived capacity as down scale delay has not been satisfied; configured delay [%s] "
                        + "last detected scale down event [%s]. Will request scale down in approximately [%s]",
                    downScaleDelay.getStringRep(),
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(scaleTimer.downScaleDetectedMillis())),
                    TimeValue.timeValueMillis(msLeftToScale).getStringRep()
                )
            )
            .build();
    }

    private boolean hasUnsatisfiedDeployments(TrainedModelAssignmentMetadata trainedModelAssignmentMetadata, List<DiscoveryNode> mlNodes) {
        final Set<String> mlNodeIds = mlNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        return trainedModelAssignmentMetadata.allAssignments()
            .values()
            .stream()
            .filter(deployment -> deployment.getTaskParams().getPriority() == Priority.NORMAL)
            .anyMatch(deployment -> deployment.isSatisfied(mlNodeIds) == false);
    }

    private MlProcessorAutoscalingCapacity.Builder computeRequiredCapacity(TrainedModelAssignmentMetadata trainedModelAssignmentMetadata) {
        int maxThreadsPerAllocation = 0;
        double processorCount = 0;
        boolean hasLowPriorityDeployments = false;
        for (TrainedModelAssignment assignment : trainedModelAssignmentMetadata.allAssignments().values()) {
            if (assignment.getTaskParams().getPriority() == Priority.LOW) {
                hasLowPriorityDeployments = true;
                continue;
            }
            int threadsPerAllocation = assignment.getTaskParams().getThreadsPerAllocation();
            maxThreadsPerAllocation = Math.max(maxThreadsPerAllocation, threadsPerAllocation);
            processorCount += assignment.getTaskParams().getNumberOfAllocations() * threadsPerAllocation;
        }

        if (hasLowPriorityDeployments) {
            // If there are low priority deployments let us ensure there will at least be one node required.
            processorCount = Math.max(0.1, processorCount);
        }

        return MlProcessorAutoscalingCapacity.builder(
            maxThreadsPerAllocation > 0 ? Processors.of(Double.valueOf(maxThreadsPerAllocation)) : Processors.ZERO,
            processorCount > 0 ? Processors.of(Double.valueOf(processorCount)) : Processors.ZERO
        );
    }

    MlProcessorAutoscalingCapacity computeCurrentCapacity(List<DiscoveryNode> mlNodes) {
        Processors maxNodeProcessors = Processors.ZERO;
        Processors tierProcessors = Processors.ZERO;
        for (DiscoveryNode node : mlNodes) {
            Processors nodeProcessors = MlProcessors.get(node);
            if (nodeProcessors.compareTo(maxNodeProcessors) > 0) {
                maxNodeProcessors = nodeProcessors;
            }
            tierProcessors = tierProcessors.plus(nodeProcessors);
        }
        return MlProcessorAutoscalingCapacity.builder(maxNodeProcessors, tierProcessors).build();
    }
}
