/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class LifecyclePolicy extends AbstractDiffable<LifecyclePolicy> implements ToXContentObject, Writeable {
    private static final Logger logger = ESLoggerFactory.getLogger(LifecyclePolicy.class);

    public static final ParseField PHASES_FIELD = new ParseField("phases");

    @SuppressWarnings("unchecked")
    private static ConstructingObjectParser<LifecyclePolicy, Tuple<String, NamedXContentRegistry>> PARSER = new ConstructingObjectParser<>(
            "lifecycle_policy", false, (a, c) -> new LifecyclePolicy(c.v1(), (List<Phase>) a[0]));

    static {
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> Phase.parse(p, new Tuple<>(n, c.v2())),
                v -> {
                    throw new IllegalArgumentException("ordered " + PHASES_FIELD.getPreferredName() + " are not supported");
                }, PHASES_FIELD);
    }

    public static LifecyclePolicy parse(XContentParser parser, Tuple<String, NamedXContentRegistry> context) {
        return PARSER.apply(parser, context);
    }

    private final String name;
    private final List<Phase> phases;

    public LifecyclePolicy(String name, List<Phase> phases) {
        this.name = name;
        this.phases = phases;
    }

    public LifecyclePolicy(StreamInput in) throws IOException {
        name = in.readString();
        phases = in.readList(Phase::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeList(phases);
    }

    public String getName() {
        return name;
    }

    public List<Phase> getPhases() {
        return phases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(PHASES_FIELD.getPreferredName());
        for (Phase phase : phases) {
            builder.field(phase.getName(), phase);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public void execute(IndexMetaData idxMeta, Client client) {
        String currentPhaseName = IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.get(idxMeta.getSettings());
        boolean currentPhaseActionsComplete = IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.get(idxMeta.getSettings())
                .equals(Phase.PHASE_COMPLETED);
        String indexName = idxMeta.getIndex().getName();
        if (Strings.isNullOrEmpty(currentPhaseName)) {
            String firstPhaseName = phases.get(0).getName();
            client.admin().indices().prepareUpdateSettings(indexName)
                    .setSettings(Settings.builder().put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), firstPhaseName))
                    .execute(new ActionListener<UpdateSettingsResponse>() {

                        @Override
                        public void onResponse(UpdateSettingsResponse response) {
                            if (response.isAcknowledged()) {
                                logger.info("Successfully initialised phase [" + firstPhaseName + "] for index [" + indexName + "]");
                            } else {
                                logger.error("Failed to initialised phase [" + firstPhaseName + "] for index [" + indexName + "]");
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.error("Failed to initialised phase [" + firstPhaseName + "] for index [" + indexName + "]", e);
                        }
                    });
        } else if (currentPhaseActionsComplete) {
            int currentPhaseIndex = -1;
            for (int i = 0; i < phases.size(); i++) {
                if (phases.get(i).getName().equals(currentPhaseName)) {
                    currentPhaseIndex = i;
                    break;
                }
            }
            if (currentPhaseIndex < phases.size() - 1) {
                Phase nextPhase = phases.get(currentPhaseIndex + 1);
                if (nextPhase.canExecute(idxMeta)) {
                    String nextPhaseName = nextPhase.getName();
                    client.admin().indices().prepareUpdateSettings(indexName)
                            .setSettings(Settings.builder()
                                    .put(IndexLifecycle.LIFECYCLE_TIMESERIES_PHASE_SETTING.getKey(), nextPhaseName)
                                    .put(IndexLifecycle.LIFECYCLE_TIMESERIES_ACTION_SETTING.getKey(), ""))
                            .execute(new ActionListener<UpdateSettingsResponse>() {

                                @Override
                                public void onResponse(UpdateSettingsResponse response) {
                                    if (response.isAcknowledged()) {
                                        logger.info(
                                                "Successfully initialised phase [" + nextPhaseName + "] for index [" + indexName + "]");
                                    } else {
                                        logger.error("Failed to initialised phase [" + nextPhaseName + "] for index [" + indexName + "]");
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.error("Failed to initialised phase [" + nextPhaseName + "] for index [" + indexName + "]", e);
                                }
                    });
                }
            }
        } else {
            Phase currentPhase = phases.stream().filter(phase -> phase.getName().equals(currentPhaseName)).findAny()
                    .orElseThrow(() -> new IllegalStateException("Current phase [" + currentPhaseName + "] not found in lifecycle ["
                            + getName() + "] for index [" + indexName + "]"));
            currentPhase.execute(idxMeta, client);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, phases);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        LifecyclePolicy other = (LifecyclePolicy) obj;
        return Objects.equals(name, other.name) && Objects.equals(phases, other.phases);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
