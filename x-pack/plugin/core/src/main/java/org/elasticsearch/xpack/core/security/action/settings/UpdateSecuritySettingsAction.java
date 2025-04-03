/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.settings;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateSecuritySettingsAction {

    public static final ActionType<AcknowledgedResponse> INSTANCE = new ActionType<>("cluster:admin/xpack/security/settings/update");

    // The names here are separate constants for 2 reasons:
    // 1. Keeping the names defined here helps ensure REST compatibility, even if the internal aliases of these indices change,
    // 2. The actual constants for these indices are in the security package, whereas this class is in core
    public static final String MAIN_INDEX_NAME = "security";
    public static final String TOKENS_INDEX_NAME = "security-tokens";
    public static final String PROFILES_INDEX_NAME = "security-profile";

    /**
     * A map of allowed settings to validators for those settings. Values should take the value which is being assigned to the setting
     * and an existing {@link ActionRequestValidationException}, to which they should add if the value is disallowed.
     */
    public static final Map<
        String,
        BiFunction<Object, ActionRequestValidationException, ActionRequestValidationException>> ALLOWED_SETTING_VALIDATORS = Map.of(
            IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
            (it, ex) -> ex, // no additional validation
            IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS,
            (it, ex) -> ex, // no additional validation
            DataTier.TIER_PREFERENCE,
            (it, ex) -> {
                Set<String> allowedTiers = Set.of(DataTier.DATA_CONTENT, DataTier.DATA_HOT, DataTier.DATA_WARM, DataTier.DATA_COLD);
                if (it instanceof String preference) {
                    String disallowedTiers = DataTier.parseTierList(preference)
                        .stream()
                        .filter(tier -> allowedTiers.contains(tier) == false)
                        .collect(Collectors.joining(","));
                    if (disallowedTiers.isEmpty() == false) {
                        return ValidateActions.addValidationError(
                            "disallowed data tiers [" + disallowedTiers + "] found, allowed tiers are [" + String.join(",", allowedTiers),
                            ex
                        );
                    }
                }
                return ex;
            }
        );

    private UpdateSecuritySettingsAction() {/* no instances */}

    public static class Request extends AcknowledgedRequest<Request> {

        private final Map<String, Object> mainIndexSettings;
        private final Map<String, Object> tokensIndexSettings;
        private final Map<String, Object> profilesIndexSettings;

        public interface Factory {
            Request create(
                Map<String, Object> mainIndexSettings,
                Map<String, Object> tokensIndexSettings,
                Map<String, Object> profilesIndexSettings
            );
        }

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Factory> PARSER = new ConstructingObjectParser<>(
            "update_security_settings_request",
            false,
            (a, factory) -> factory.create((Map<String, Object>) a[0], (Map<String, Object>) a[1], (Map<String, Object>) a[2])
        );

        static {
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField(MAIN_INDEX_NAME));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField(TOKENS_INDEX_NAME));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField(PROFILES_INDEX_NAME));
        }

        public Request(
            TimeValue masterNodeTimeout,
            TimeValue ackTimeout,
            Map<String, Object> mainIndexSettings,
            Map<String, Object> tokensIndexSettings,
            Map<String, Object> profilesIndexSettings
        ) {
            super(masterNodeTimeout, ackTimeout);
            this.mainIndexSettings = Objects.requireNonNullElse(mainIndexSettings, Collections.emptyMap());
            this.tokensIndexSettings = Objects.requireNonNullElse(tokensIndexSettings, Collections.emptyMap());
            this.profilesIndexSettings = Objects.requireNonNullElse(profilesIndexSettings, Collections.emptyMap());
        }

        public static Request readFrom(StreamInput in) throws IOException {
            return new Request(in);
        }

        private Request(StreamInput in) throws IOException {
            super(in);
            this.mainIndexSettings = in.readGenericMap();
            this.tokensIndexSettings = in.readGenericMap();
            this.profilesIndexSettings = in.readGenericMap();
        }

        private Request(TimeValue masterNodeTimeout, TimeValue ackTimeout, StreamInput in) throws IOException {
            super(masterNodeTimeout, ackTimeout);
            this.mainIndexSettings = in.readGenericMap();
            this.tokensIndexSettings = in.readGenericMap();
            this.profilesIndexSettings = in.readGenericMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                super.writeTo(out);
            }
            out.writeGenericMap(this.mainIndexSettings);
            out.writeGenericMap(this.tokensIndexSettings);
            out.writeGenericMap(this.profilesIndexSettings);
        }

        public static Request parse(XContentParser parser, Factory factory) {
            return PARSER.apply(parser, factory);
        }

        public Map<String, Object> mainIndexSettings() {
            return this.mainIndexSettings;
        }

        public Map<String, Object> tokensIndexSettings() {
            return this.tokensIndexSettings;
        }

        public Map<String, Object> profilesIndexSettings() {
            return this.profilesIndexSettings;
        }

        @Override
        public ActionRequestValidationException validate() {
            if (mainIndexSettings.isEmpty() && tokensIndexSettings.isEmpty() && profilesIndexSettings.isEmpty()) {
                return ValidateActions.addValidationError("No settings given to update", null);
            }
            ActionRequestValidationException validationException = validateIndexSettings(mainIndexSettings, MAIN_INDEX_NAME, null);
            validationException = validateIndexSettings(tokensIndexSettings, TOKENS_INDEX_NAME, validationException);
            validationException = validateIndexSettings(profilesIndexSettings, PROFILES_INDEX_NAME, validationException);
            return validationException;
        }

        private static ActionRequestValidationException validateIndexSettings(
            Map<String, Object> indexSettings,
            String indexName,
            ActionRequestValidationException existingExceptions
        ) {
            ActionRequestValidationException errors = existingExceptions;

            for (Map.Entry<String, Object> entry : indexSettings.entrySet()) {
                String setting = entry.getKey();
                if (ALLOWED_SETTING_VALIDATORS.containsKey(setting)) {
                    errors = ALLOWED_SETTING_VALIDATORS.get(setting).apply(entry.getValue(), errors);
                } else {
                    errors = ValidateActions.addValidationError(
                        "illegal setting for index ["
                            + indexName
                            + "]: ["
                            + setting
                            + "], this setting may not be configured. Only the following settings may be configured for that index: "
                            + ALLOWED_SETTING_VALIDATORS.keySet(),
                        existingExceptions
                    );
                }
            }

            return errors;
        }
    }
}
