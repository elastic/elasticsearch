/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.elser;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.inference.services.MapParsingUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public record HuggingFaceElserServiceSettings(String url) implements ServiceSettings {
    public static final String NAME = "hugging_face_elser_service_settings";

    private static final Logger logger = LogManager.getLogger(HuggingFaceElserServiceSettings.class);
    static final String URL = "url";

    public static HuggingFaceElserServiceSettings fromMap(Map<String, Object> map) {
        ValidationException validationException = new ValidationException();

        String parsedUrl = MapParsingUtils.removeAsType(map, URL, String.class);
        if (parsedUrl == null) {
            validationException.addValidationError(MapParsingUtils.missingSettingErrorMsg(URL, ModelConfigurations.SERVICE_SETTINGS));
        }

        try {
            validateUrl(parsedUrl);
        } catch (IllegalArgumentException ignored) {
            validationException.addValidationError(MapParsingUtils.invalidUrlErrorMsg(parsedUrl, ModelConfigurations.SERVICE_SETTINGS));
        }

        if (validationException.validationErrors().isEmpty() == false) {
            throw validationException;
        }

        return new HuggingFaceElserServiceSettings(parsedUrl);
    }

    // TODO move this to a common location and potentially improve parsing errors
    private static void validateUrl(String url) throws IllegalArgumentException {
        if (url == null) {
            return;
        }

        try {
            new URI(url);
        } catch (URISyntaxException e) {
            logger.info(format("Invalid URL received [%s]", url), e);
            throw new IllegalArgumentException(format("unable to parse url [%s]", url), e);
        }
    }

    public HuggingFaceElserServiceSettings {
        Objects.requireNonNull(url);
    }

    public HuggingFaceElserServiceSettings(StreamInput in) throws IOException {
        this(in.readString());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(URL, url);
        builder.endObject();

        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ML_INFERENCE_TASK_SETTINGS_OPTIONAL_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(url);
    }
}
