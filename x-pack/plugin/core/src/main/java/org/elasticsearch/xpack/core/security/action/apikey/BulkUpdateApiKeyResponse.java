/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

// TODO consider builder
public final class BulkUpdateApiKeyResponse extends ActionResponse implements ToXContentObject, Writeable {
    private final List<String> updated;
    private final List<String> noops;

    // TODO
    private final List<ElasticsearchException> errors;

    public BulkUpdateApiKeyResponse() {
        this.updated = new ArrayList<>();
        this.noops = new ArrayList<>();
        this.errors = new ArrayList<>();
    }

    public BulkUpdateApiKeyResponse(final List<String> updated, final List<String> noops, final List<ElasticsearchException> errors) {
        this.updated = updated;
        this.noops = noops;
        this.errors = errors;
    }

    public BulkUpdateApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.updated = in.readStringList();
        this.noops = in.readStringList();
        this.errors = in.readList(StreamInput::readException);
    }

    public List<String> getUpdated() {
        return updated;
    }

    public List<String> getNoops() {
        return noops;
    }

    public List<ElasticsearchException> getErrors() {
        return errors;
    }

    // TODO builder?

    public void addToUpdated(final String id) {
        updated.add(id);
    }

    public void addToNoops(final String id) {
        noops.add(id);
    }

    public void addToErrors(final String id, final ElasticsearchException ex) {
        errors.add(ex);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

}
