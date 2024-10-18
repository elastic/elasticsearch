/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.watcher.transport.actions.put;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class UpdateWatcherSettingsAction extends ActionType<AcknowledgedResponse> {

    public static final UpdateWatcherSettingsAction INSTANCE = new UpdateWatcherSettingsAction();
    public static final String NAME = "cluster:admin/xpack/watcher/settings/update";

    public static final Set<String> ALLOWED_SETTING_KEYS = Set.of(
        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
        IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS
    );

    public UpdateWatcherSettingsAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        private final Map<String, Object> settings;

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, Map<String, Object> settings) {
            super(masterNodeTimeout, ackTimeout);
            this.settings = settings;
        }

        public static Request readFrom(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                return new Request(in);
            } else {
                return new Request(TimeValue.THIRTY_SECONDS, TimeValue.THIRTY_SECONDS, in);
            }
        }

        private Request(StreamInput in) throws IOException {
            super(in);
            this.settings = in.readGenericMap();
        }

        @UpdateForV9(owner = UpdateForV9.Owner.DATA_MANAGEMENT) // bwc no longer required
        private Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, StreamInput in) throws IOException {
            super(masterNodeTimeout, ackTimeout);
            this.settings = in.readGenericMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                super.writeTo(out);
            }
            out.writeGenericMap(this.settings);
        }

        public Map<String, Object> settings() {
            return this.settings;
        }

        @Override
        public ActionRequestValidationException validate() {
            Set<String> forbiddenSettings = Sets.difference(settings.keySet(), ALLOWED_SETTING_KEYS);
            if (forbiddenSettings.size() > 0) {
                return ValidateActions.addValidationError(
                    "illegal settings: "
                        + forbiddenSettings
                        + ", these settings may not be configured. Only the following settings may be configured: "
                        + ALLOWED_SETTING_KEYS,
                    null
                );
            }
            return null;
        }
    }
}
