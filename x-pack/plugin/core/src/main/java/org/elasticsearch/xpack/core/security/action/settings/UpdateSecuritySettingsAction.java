/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.settings;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class UpdateSecuritySettingsAction extends ActionType<AcknowledgedResponse> {
    public static final UpdateSecuritySettingsAction INSTANCE = new UpdateSecuritySettingsAction();
    public static final String NAME = "cluster:admin/xpack/security/settings/update";

    // The names here are separate constants for 2 reasons:
    // 1. Keeping the names defined here helps ensure REST compatibility, even if the internal aliases of these indices change,
    // 2. The actual constants for these indices are in the security package, whereas this class is in core
    public static final String MAIN_INDEX_NAME = "security";
    public static final String TOKENS_INDEX_NAME = "security-tokens";
    public static final String PROFILES_INDEX_NAME = "security-profile";

    public static final Set<String> ALLOWED_SETTING_KEYS = Set.of(
        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
        IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS
    );

    public UpdateSecuritySettingsAction() {
        super(NAME, AcknowledgedResponse::readFrom);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        private final Map<String, Object> mainIndexSettings;
        private final Map<String, Object> tokensIndexSettings;
        private final Map<String, Object> profilesIndexSettings;

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Request, Void> PARSER = new ConstructingObjectParser<>(
            "update_security_settings_request",
            false,
            a -> new Request((Map<String, Object>) a[0], (Map<String, Object>) a[1], (Map<String, Object>) a[2])
        );

        static {
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField(MAIN_INDEX_NAME));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField(TOKENS_INDEX_NAME));
            PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.map(), new ParseField(PROFILES_INDEX_NAME));
        }

        public Request(
            Map<String, Object> mainIndexSettings,
            Map<String, Object> tokensIndexSettings,
            Map<String, Object> profilesIndexSettings
        ) {
            this.mainIndexSettings = Objects.requireNonNullElse(mainIndexSettings, Collections.emptyMap());
            this.tokensIndexSettings = Objects.requireNonNullElse(tokensIndexSettings, Collections.emptyMap());
            this.profilesIndexSettings = Objects.requireNonNullElse(profilesIndexSettings, Collections.emptyMap());
        }

        public Request(StreamInput in) throws IOException {
            this.mainIndexSettings = in.readMap();
            this.tokensIndexSettings = in.readMap();
            this.profilesIndexSettings = in.readMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeGenericMap(this.mainIndexSettings);
            out.writeGenericMap(this.tokensIndexSettings);
            out.writeGenericMap(this.profilesIndexSettings);
        }

        public static Request parse(XContentParser parser) {
            return PARSER.apply(parser, null);
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
            ActionRequestValidationException validationException = validateIndexSettings(mainIndexSettings, MAIN_INDEX_NAME, null);
            validationException = validateIndexSettings(tokensIndexSettings, TOKENS_INDEX_NAME, validationException);
            validationException = validateIndexSettings(profilesIndexSettings, PROFILES_INDEX_NAME, validationException);
            if (validationException != null) return validationException;
            return null;
        }

        private static ActionRequestValidationException validateIndexSettings(
            Map<String, Object> indexSettings,
            String indexName,
            ActionRequestValidationException existingExceptions
        ) {
            if (indexSettings.containsKey("index") == false) {
                // return ValidateActions.addValidationError("only [index] settings may be set")
            }
            Set<String> forbiddenSettings = Sets.difference(indexSettings.keySet(), ALLOWED_SETTING_KEYS);
            if (forbiddenSettings.size() > 0) {
                return ValidateActions.addValidationError(
                    "illegal settings for index ["
                        + indexName
                        + "]: "
                        + forbiddenSettings
                        + ", these settings may not be configured. Only the following settings may be configured for that index: "
                        + ALLOWED_SETTING_KEYS,
                    existingExceptions
                );
            }
            return null;
        }
    }
}
