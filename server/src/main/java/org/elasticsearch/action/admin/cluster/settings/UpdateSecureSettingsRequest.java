/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Secure setting values to set, keyed by setting name. A {@code null} value removes the API-set value for that name.
 * <p>Values are never part of {@link #toString()} or {@link #getDescription()}, so they can't end up in logs or task listings.
 */
public class UpdateSecureSettingsRequest extends AcknowledgedRequest<UpdateSecureSettingsRequest> {

    private final Map<String, SecureString> settings;

    public UpdateSecureSettingsRequest(TimeValue masterNodeTimeout, TimeValue ackTimeout, Map<String, SecureString> settings) {
        super(masterNodeTimeout, ackTimeout);
        this.settings = Collections.unmodifiableMap(new TreeMap<>(settings));
    }

    public UpdateSecureSettingsRequest(StreamInput in) throws IOException {
        super(in);
        this.settings = Collections.unmodifiableMap(
            new TreeMap<>(in.readMap(StreamInput::readString, StreamInput::readOptionalSecureString))
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeMap(settings, StreamOutput::writeString, StreamOutput::writeOptionalSecureString);
    }

    /**
     * The values to set, keyed by setting name. A {@code null} value removes the setting.
     */
    public Map<String, SecureString> settings() {
        return settings;
    }

    public Set<String> settingNames() {
        return settings.keySet();
    }

    @Override
    public ActionRequestValidationException validate() {
        if (settings.isEmpty()) {
            return addValidationError("no secure settings to update", null);
        }
        return null;
    }

    @Override
    public String getDescription() {
        return "update secure settings " + settings.keySet();
    }

    @Override
    public String toString() {
        return "UpdateSecureSettingsRequest{settings=" + settings.keySet() + "}";
    }
}
