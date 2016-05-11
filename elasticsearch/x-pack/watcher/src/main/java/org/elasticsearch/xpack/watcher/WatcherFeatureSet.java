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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.XPackFeatureSet;

import java.io.IOException;

/**
 *
 */
public class WatcherFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final WatcherLicensee licensee;

    @Inject
    public WatcherFeatureSet(Settings settings, @Nullable WatcherLicensee licensee, NamedWriteableRegistry namedWriteableRegistry) {
        this.enabled = Watcher.enabled(settings);
        this.licensee = licensee;
        namedWriteableRegistry.register(Usage.class, Usage.WRITEABLE_NAME, Usage::new);
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
    public Usage usage() {
        return new Usage(available(), enabled());
    }

    static class Usage extends XPackFeatureSet.Usage {

        private static final String WRITEABLE_NAME = writeableName(Watcher.NAME);

        public Usage(StreamInput input) throws IOException {
            super(input);
        }

        public Usage(boolean available, boolean enabled) {
            super(Watcher.NAME, available, enabled);
        }

        @Override
        public String getWriteableName() {
            return WRITEABLE_NAME;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                    .field(Field.AVAILABLE, available)
                    .field(Field.ENABLED, enabled)
                    .endObject();

        }
    }
}
