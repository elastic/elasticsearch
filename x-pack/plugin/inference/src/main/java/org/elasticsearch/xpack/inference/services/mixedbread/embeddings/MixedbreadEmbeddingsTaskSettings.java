/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mixedbread.embeddings;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalString;
import static org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioConstants.USER_FIELD;

public class MixedbreadEmbeddingsTaskSettings implements TaskSettings {
    public static final String NAME = "mixedbread_embeddings_task_settings";

    public static MixedbreadEmbeddingsTaskSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String user = extractOptionalString(map, USER_FIELD, ModelConfigurations.TASK_SETTINGS, validationException);
        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new MixedbreadEmbeddingsTaskSettings(user);
    }

    /**
     * Creates a new {@link MixedbreadEmbeddingsTaskSettings} object by overriding the values in originalSettings with the ones
     * passed in via requestSettings if the fields are not null.
     *
     * @param originalSettings the original {@link MixedbreadEmbeddingsTaskSettings} from the inference entity configuration from storage
     * @param requestSettings  the {@link MixedbreadEmbeddingsRequestTaskSettings} from the request
     * @return a new {@link MixedbreadEmbeddingsTaskSettings}
     */
    public static MixedbreadEmbeddingsTaskSettings of(
        MixedbreadEmbeddingsTaskSettings originalSettings,
        MixedbreadEmbeddingsRequestTaskSettings requestSettings
    ) {
        var userToUse = requestSettings.user() == null ? originalSettings.user : requestSettings.user();
        return new MixedbreadEmbeddingsTaskSettings(userToUse);
    }

    public MixedbreadEmbeddingsTaskSettings(@Nullable String user) {
        this.user = user;
    }

    public MixedbreadEmbeddingsTaskSettings(StreamInput in) throws IOException {
        this.user = in.readOptionalString();
    }

    private final String user;

    public String user() {
        return this.user;
    }

    @Override
    public boolean isEmpty() {
        return user == null || user.isEmpty();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_14_0;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(this.user);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (user != null) {
            builder.field(USER_FIELD, user);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MixedbreadEmbeddingsTaskSettings that = (MixedbreadEmbeddingsTaskSettings) o;
        return Objects.equals(user, that.user);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(user);
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        MixedbreadEmbeddingsRequestTaskSettings requestSettings = MixedbreadEmbeddingsRequestTaskSettings.fromMap(
            new HashMap<>(newSettings)
        );
        return MixedbreadEmbeddingsTaskSettings.of(this, requestSettings);
    }
}
