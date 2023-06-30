/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.put;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.common.settings.Settings.readSettingsFromStream;

/**
 * Register repository request.
 * <p>
 * Registers a repository with given name, type and settings. If the repository with the same name already
 * exists in the cluster, the new repository will replace the existing repository.
 */
public class PutRepositoryRequest extends AcknowledgedRequest<PutRepositoryRequest> implements ToXContentObject {

    private String name;

    private String type;

    private boolean verify = true;

    private Settings settings = Settings.EMPTY;

    public PutRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        type = in.readString();
        settings = readSettingsFromStream(in);
        verify = in.readBoolean();
    }

    public PutRepositoryRequest() {}

    /**
     * Constructs a new put repository request with the provided name.
     */
    public PutRepositoryRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        return validationException;
    }

    /**
     * Sets the name of the repository.
     *
     * @param name repository name
     */
    public PutRepositoryRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The name of the repository.
     *
     * @return repository name
     */
    public String name() {
        return this.name;
    }

    /**
     * The type of the repository
     * <ul>
     * <li>"fs" - shared filesystem repository</li>
     * </ul>
     *
     * @param type repository type
     * @return this request
     */
    public PutRepositoryRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Returns repository type
     *
     * @return repository type
     */
    public String type() {
        return this.type;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets the repository settings.
     *
     * @param source repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Map<String, Object> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Returns repository settings
     *
     * @return repository settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Sets whether or not the repository should be verified after creation
     */
    public PutRepositoryRequest verify(boolean verify) {
        this.verify = verify;
        return this;
    }

    /**
     * Returns true if repository should be verified after creation
     */
    public boolean verify() {
        return this.verify;
    }

    /**
     * Parses repository definition.
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(Map<String, Object> repositoryDefinition) {
        for (Map.Entry<String, Object> entry : repositoryDefinition.entrySet()) {
            String name = entry.getKey();
            if (name.equals("type")) {
                type(entry.getValue().toString());
            } else if (name.equals("settings")) {
                if ((entry.getValue() instanceof Map) == false) {
                    throw new IllegalArgumentException("Malformed settings section, should include an inner object");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> sub = (Map<String, Object>) entry.getValue();
                settings(sub);
            }
        }
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(type);
        settings.writeTo(out);
        out.writeBoolean(verify);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("type", type);

        builder.startObject("settings");
        settings.toXContent(builder, params);
        builder.endObject();

        builder.field("verify", verify);
        builder.endObject();
        return builder;
    }
}
