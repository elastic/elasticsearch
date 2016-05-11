/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.Monitoring;
import org.elasticsearch.xpack.XPackFeatureSet;

import java.io.IOException;

/**
 *
 */
public class SecurityFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final SecurityLicenseState licenseState;

    @Inject
    public SecurityFeatureSet(Settings settings, @Nullable SecurityLicenseState licenseState,
                              NamedWriteableRegistry namedWriteableRegistry) {
        this.enabled = Security.enabled(settings);
        this.licenseState = licenseState;
        namedWriteableRegistry.register(Usage.class, Usage.WRITEABLE_NAME, Usage::new);
    }

    @Override
    public String name() {
        return Security.NAME;
    }

    @Override
    public String description() {
        return "Security for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.authenticationAndAuthorizationEnabled();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public XPackFeatureSet.Usage usage() {
        return new Usage(available(), enabled());
    }

    static class Usage extends XPackFeatureSet.Usage {

        private static final String WRITEABLE_NAME = writeableName(Security.NAME);

        public Usage(StreamInput input) throws IOException {
            super(input);
        }

        public Usage(boolean available, boolean enabled) {
            super(Security.NAME, available, enabled);
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
