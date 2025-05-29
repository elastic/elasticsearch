/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation.Error;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * The request that allows to set a user as enabled or disabled
 */
public class SetEnabledRequest extends LegacyActionRequest implements UserRequest, WriteRequest<SetEnabledRequest> {

    private Boolean enabled;
    private String username;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public SetEnabledRequest() {}

    public SetEnabledRequest(StreamInput in) throws IOException {
        super(in);
        this.enabled = in.readBoolean();
        this.username = in.readString();
        this.refreshPolicy = RefreshPolicy.readFrom(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        Error error = NativeRealmValidationUtil.validateUsername(username, true, Settings.EMPTY);
        if (error != null) {
            validationException = addValidationError(error.toString(), validationException);
        }
        if (enabled == null) {
            validationException = addValidationError("enabled must be set", validationException);
        }
        return validationException;
    }

    /**
     * @return whether the user should be set to enabled or not
     */
    public Boolean enabled() {
        return enabled;
    }

    /**
     * Set whether the user should be enabled or not.
     */
    public void enabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return the username that this request applies to.
     */
    public String username() {
        return username;
    }

    /**
     * Set the username that the request applies to. Must not be {@code null}
     */
    public void username(String username) {
        this.username = username;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}, the default), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}).
     */
    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public SetEnabledRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(enabled);
        out.writeString(username);
        refreshPolicy.writeTo(out);
    }
}
