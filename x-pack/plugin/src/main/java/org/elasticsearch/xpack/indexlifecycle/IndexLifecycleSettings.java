/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IndexLifecycleSettings implements ToXContentObject {

    public static final ParseField PHASES_FIELD = new ParseField("phases");

    private List<Phase> phases;

    public IndexLifecycleSettings(Index index, long indexCreationDate, Settings settings, Client client) {
        phases = new ArrayList<>();
        for (Map.Entry<String, Settings> e : settings.getAsGroups().entrySet()) {
            Phase phase = new Phase(e.getKey(), index, indexCreationDate, e.getValue(), client);
            phases.add(phase);
        }
    }

    public IndexLifecycleSettings(List<Phase> phases) {
        this.phases = phases;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.array(PHASES_FIELD.getPreferredName(), phases);
        builder.endObject();
        return builder;
    }

    public void schedulePhases(SchedulerEngine scheduler) {
        for (Phase phase : phases) {
            scheduler.register(phase);
            scheduler.add(phase);
            ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN")
                    .error("kicked off lifecycle job to be triggered in " + phase.getAfter() + " seconds");
        }
    }

    public static void main(String[] args) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put("new.after", "1m");
        settingsBuilder.put("delete.after", "3s");
        settingsBuilder.put("delete.actions.delete.what", "me");
        Settings settings = settingsBuilder.build();
        System.out.println(settings);

        long currentTimeMillis = System.currentTimeMillis();
        System.out.println(currentTimeMillis);

        IndexLifecycleSettings lifecycleSettings = new IndexLifecycleSettings(new Index("test_index", "1234567890"), currentTimeMillis,
                settings, null);
        System.out.println(Strings.toString(lifecycleSettings));
    }

}
