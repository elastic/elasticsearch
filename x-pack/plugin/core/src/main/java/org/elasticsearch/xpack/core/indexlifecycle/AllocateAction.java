/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AllocateAction implements LifecycleAction {

    public static final String NAME = "allocate";
    public static final ParseField INCLUDE_FIELD = new ParseField("include");
    public static final ParseField EXCLUDE_FIELD = new ParseField("exclude");
    public static final ParseField REQUIRE_FIELD = new ParseField("require");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AllocateAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new AllocateAction((Map<String, String>) a[0], (Map<String, String>) a[1], (Map<String, String>) a[2]));

    static {
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), INCLUDE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), EXCLUDE_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.mapStrings(), REQUIRE_FIELD);
    }

    private final Map<String, String> include;
    private final Map<String, String> exclude;
    private final Map<String, String> require;

    public static AllocateAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public AllocateAction(Map<String, String> include, Map<String, String> exclude, Map<String, String> require) {
        if (include == null) {
            this.include = Collections.emptyMap();
        } else {
            this.include = include;
        }
        if (exclude == null) {
            this.exclude = Collections.emptyMap();
        } else {
            this.exclude = exclude;
        }
        if (require == null) {
            this.require = Collections.emptyMap();
        } else {
            this.require = require;
        }
        if (this.include.isEmpty() && this.exclude.isEmpty() && this.require.isEmpty()) {
            throw new IllegalArgumentException(
                    "At least one of " + INCLUDE_FIELD.getPreferredName() + ", " + EXCLUDE_FIELD.getPreferredName() + " or "
                            + REQUIRE_FIELD.getPreferredName() + "must contain attributes for action " + NAME);
        }
    }

    @SuppressWarnings("unchecked")
    public AllocateAction(StreamInput in) throws IOException {
        this((Map<String, String>) in.readGenericValue(), (Map<String, String>) in.readGenericValue(),
                (Map<String, String>) in.readGenericValue());
    }

    public Map<String, String> getInclude() {
        return include;
    }

    public Map<String, String> getExclude() {
        return exclude;
    }

    public Map<String, String> getRequire() {
        return require;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeGenericValue(include);
        out.writeGenericValue(exclude);
        out.writeGenericValue(require);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INCLUDE_FIELD.getPreferredName(), include);
        builder.field(EXCLUDE_FIELD.getPreferredName(), exclude);
        builder.field(REQUIRE_FIELD.getPreferredName(), require);
        builder.endObject();
        return builder;
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
        include.forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_INCLUDE_GROUP_SETTING.getKey() + key, value));
        exclude.forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + key, value));
        require.forEach((key, value) -> newSettings.put(IndexMetaData.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + key, value));
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

    @Override
    public int hashCode() {
        return Objects.hash(include, exclude, require);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        AllocateAction other = (AllocateAction) obj;
        return Objects.equals(include, other.include) && Objects.equals(exclude, other.exclude) && Objects.equals(require, other.require);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
