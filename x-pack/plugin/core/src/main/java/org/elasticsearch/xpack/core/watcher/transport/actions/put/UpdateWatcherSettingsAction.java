/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.watcher.transport.actions.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class UpdateWatcherSettingsAction extends ActionType<AcknowledgedResponse> {

    public static final UpdateWatcherSettingsAction INSTANCE = new UpdateWatcherSettingsAction();
    public static final String NAME = "cluster:admin/xpack/watcher/settings/update";

    public static final Set<String> ALLOWED_SETTING_KEYS = Set.of(
        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
        IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS
    );

    public static final Set<String> ALLOWED_SETTINGS_PREFIXES = Set.of(
        IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX,
        IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX,
        IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_PREFIX
    );

    public static final Set<String> EXPLICITLY_DENIED_SETTINGS = Set.of(
        IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._tier_preference"
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

        public Request(StreamInput in) throws IOException {
            super(in);
            this.settings = in.readGenericMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeGenericMap(this.settings);
        }

        public Map<String, Object> settings() {
            return this.settings;
        }

        @Override
        public ActionRequestValidationException validate() {
            Set<String> forbiddenSettings = settings.keySet()
                .stream()
                .filter(
                    setting -> (ALLOWED_SETTING_KEYS.contains(setting) == false
                        && ALLOWED_SETTINGS_PREFIXES.stream().noneMatch(prefix -> setting.startsWith(prefix + ".")))
                        || EXPLICITLY_DENIED_SETTINGS.contains(setting)
                )
                .collect(Collectors.toSet());

            if (forbiddenSettings.isEmpty() == false) {
                return ValidateActions.addValidationError(
                    "illegal settings: "
                        + forbiddenSettings
                        + ", these settings may not be configured. Only the following settings may be configured: "
                        + ALLOWED_SETTING_KEYS
                        + ", "
                        + ALLOWED_SETTINGS_PREFIXES.stream().map(s -> s + ".*").collect(Collectors.toSet())
                        + " excluding the following explicitly denied settings: "
                        + EXPLICITLY_DENIED_SETTINGS,
                    null
                );
            }
            return null;
        }
    }
}
