/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.XPackFeatureSet;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
public class WatcherFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final WatcherLicensee licensee;
    private final WatcherService watcherService;

    @Inject
    public WatcherFeatureSet(Settings settings, @Nullable WatcherLicensee licensee, NamedWriteableRegistry namedWriteableRegistry,
                             @Nullable WatcherService watcherService) {
        this.watcherService = watcherService;
        this.enabled = Watcher.enabled(settings);
        this.licensee = licensee;
        namedWriteableRegistry.register(Usage.class, Usage.writeableName(Watcher.NAME), Usage::new);
    }

    @Override
    public String name() {
        return Watcher.NAME;
    }

    @Override
    public String description() {
        return "Alerting, Notification and Automation for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licensee != null && licensee.isAvailable();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public XPackFeatureSet.Usage usage() {
        return new Usage(available(), enabled(), watcherService != null ? watcherService.usageStats() : Collections.emptyMap());
    }

    static class Usage extends XPackFeatureSet.Usage {

        private final Map<String, Object> stats;

        public Usage(StreamInput in) throws IOException {
            super(in);
            stats = in.readMap();
        }

        public Usage(boolean available, boolean enabled, Map<String, Object> stats) {
            super(Watcher.NAME, available, enabled);
            this.stats = stats;
        }

        public Map<String, Object> stats() {
            return stats;
        }

        @Override
        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
            if (enabled) {
                for (Map.Entry<String, Object> entry : stats.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(stats);
        }
    }
}
