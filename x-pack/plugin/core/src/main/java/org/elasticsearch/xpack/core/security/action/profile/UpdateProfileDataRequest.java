/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.profile;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class UpdateProfileDataRequest extends ActionRequest {

    private final String uid;
    private final Map<String, Object> labels;
    private final Map<String, Object> data;
    private final long ifPrimaryTerm;
    private final long ifSeqNo;
    private final RefreshPolicy refreshPolicy;

    public UpdateProfileDataRequest(
        String uid,
        Map<String, Object> labels,
        Map<String, Object> data,
        long ifPrimaryTerm,
        long ifSeqNo,
        RefreshPolicy refreshPolicy
    ) {
        this.uid = Objects.requireNonNull(uid, "profile uid must not be null");
        this.labels = labels != null ? labels : Map.of();
        this.data = data != null ? data : Map.of();
        this.ifPrimaryTerm = ifPrimaryTerm;
        this.ifSeqNo = ifSeqNo;
        this.refreshPolicy = refreshPolicy;
    }

    public UpdateProfileDataRequest(StreamInput in) throws IOException {
        super(in);
        this.uid = in.readString();
        this.labels = in.readMap();
        this.data = in.readMap();
        this.ifPrimaryTerm = in.readLong();
        this.ifSeqNo = in.readLong();
        this.refreshPolicy = RefreshPolicy.readFrom(in);
    }

    public String getUid() {
        return uid;
    }

    public Map<String, Object> getLabels() {
        return labels;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public long getIfPrimaryTerm() {
        return ifPrimaryTerm;
    }

    public long getIfSeqNo() {
        return ifSeqNo;
    }

    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    public Set<String> getApplicationNames() {
        final Set<String> names = new HashSet<>(labels.keySet());
        names.addAll(data.keySet());
        return Set.copyOf(names);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        final Set<String> applicationNames = getApplicationNames();
        if (applicationNames.isEmpty()) {
            validationException = addValidationError("update request is empty", validationException);
        }
        final Set<String> namesWithDot = applicationNames.stream()
            .filter(name -> name.contains("."))
            .collect(Collectors.toUnmodifiableSet());
        if (false == namesWithDot.isEmpty()) {
            validationException = addValidationError(
                "application name must not contain dot, but found [" + Strings.collectionToCommaDelimitedString(namesWithDot) + "]",
                validationException
            );
        }
        final Set<String> namesStartsWithUnderscore = applicationNames.stream()
            .filter(name -> name.startsWith("_"))
            .collect(Collectors.toUnmodifiableSet());
        if (false == namesStartsWithUnderscore.isEmpty()) {
            validationException = addValidationError(
                "application name must not start with underscore, but found ["
                    + Strings.collectionToCommaDelimitedString(namesStartsWithUnderscore)
                    + "]",
                validationException
            );
        }
        return validationException;
    }
}
