/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AllocateAction extends org.elasticsearch.protocol.xpack.indexlifecycle.AllocateAction implements LifecycleAction {

    public AllocateAction(Integer numberOfReplicas, Map<String, String> include, Map<String, String> exclude, Map<String, String> require) {
        super(numberOfReplicas, include, exclude, require);
    }

    @SuppressWarnings("unchecked")
    public AllocateAction(StreamInput in) throws IOException {
        this(in.readOptionalVInt(), (Map<String, String>) in.readGenericValue(), (Map<String, String>) in.readGenericValue(),
                (Map<String, String>) in.readGenericValue());
    }

    public static AllocateAction parse(XContentParser parser) {
        org.elasticsearch.protocol.xpack.indexlifecycle.AllocateAction clientAction =
            org.elasticsearch.protocol.xpack.indexlifecycle.AllocateAction.parse(parser);
        return new AllocateAction(clientAction.getNumberOfReplicas(), clientAction.getInclude(), clientAction.getExclude(),
            clientAction.getRequire());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(getNumberOfReplicas());
        out.writeGenericValue(getInclude());
        out.writeGenericValue(getExclude());
        out.writeGenericValue(getRequire());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey allocateKey = new StepKey(phase, NAME, NAME);
        StepKey allocationRoutedKey = new StepKey(phase, NAME, AllocationRoutedStep.NAME);

        Settings.Builder newSettings = Settings.builder();
        if (getNumberOfReplicas() != null) {
            newSettings.put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, getNumberOfReplicas());
        }
        getInclude().forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + key, value));
        getExclude().forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + key, value));
        getRequire().forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + key, value));
        UpdateSettingsStep allocateStep = new UpdateSettingsStep(allocateKey, allocationRoutedKey, client, newSettings.build());
        AllocationRoutedStep routedCheckStep = new AllocationRoutedStep(allocationRoutedKey, nextStepKey, true);
        return Arrays.asList(allocateStep, routedCheckStep);
    }

    @Override
    public List<StepKey> toStepKeys(String phase) {
        StepKey allocateKey = new StepKey(phase, NAME, NAME);
        StepKey allocationRoutedKey = new StepKey(phase, NAME, AllocationRoutedStep.NAME);
        return Arrays.asList(allocateKey, allocationRoutedKey);
    }
}
