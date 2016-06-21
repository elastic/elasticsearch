/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;
import org.elasticsearch.xpack.XPackFeatureSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 */
public class SecurityFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final SecurityLicenseState licenseState;
    private final @Nullable Realms realms;

    @Inject
    public SecurityFeatureSet(Settings settings, @Nullable SecurityLicenseState licenseState,
                              @Nullable Realms realms, NamedWriteableRegistry namedWriteableRegistry) {
        this.enabled = Security.enabled(settings);
        this.licenseState = licenseState;
        this.realms = realms;
        namedWriteableRegistry.register(Usage.class, Usage.writeableName(Security.NAME), Usage::new);
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
        List<Map<String, Object>> enabledRealms = buildEnabledRealms(realms);
        return new Usage(available(), enabled(), enabledRealms);
    }

    static List<Map<String, Object>> buildEnabledRealms(Realms realms) {
        if (realms == null) {
            return Collections.emptyList();
        }
        List<Map<String, Object>> enabledRealms = new ArrayList<>();
        for (Realm realm : realms) {
            if (realm instanceof ReservedRealm) {
                continue; // we don't need usage of this one
            }
            Map<String, Object> stats = realm.usageStats();
            enabledRealms.add(stats);
        }
        return enabledRealms;
    }

    static class Usage extends XPackFeatureSet.Usage {

        private static final String ENABLED_REALMS_XFIELD = "enabled_realms";

        private List<Map<String, Object>> enabledRealms;

        public Usage(StreamInput in) throws IOException {
            super(in);
            enabledRealms = in.readList(StreamInput::readMap);
        }

        public Usage(boolean available, boolean enabled, List<Map<String, Object>> enabledRealms) {
            super(Security.NAME, available, enabled);
            this.enabledRealms = enabledRealms;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeList(enabledRealms.stream().map((m) -> (Writeable) o -> o.writeMap(m)).collect(Collectors.toList()));
        }

        @Override
        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
            if (enabled) {
                builder.field(ENABLED_REALMS_XFIELD, enabledRealms);
            }
        }
    }
}
