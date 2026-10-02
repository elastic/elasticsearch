/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.documentextraction;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.inference.InferenceUtils;
import org.elasticsearch.xpack.inference.services.ServiceUtils;
import org.elasticsearch.xpack.inference.services.SettingsScope;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Task settings for the Elastic Inference Service {@code document_extraction} task type. They can be stored on the inference endpoint
 * and overridden per request through the {@code task_settings} field of the document extraction request body, where the request value
 * wins (see {@link #of(ElasticInferenceServiceDocumentExtractionTaskSettings, ElasticInferenceServiceDocumentExtractionTaskSettings)}).
 * The resolved settings are forwarded to the Elastic Inference Service as-is, which maps them onto the options of the underlying
 * provider (Jina Reader):
 * <pre>
 * {
 *   "output_format": "markdown",
 *   "css": {
 *     "extract_only": [".main-content", "#post-body"],
 *     "remove": ["nav", "footer"]
 *   }
 * }</pre>
 * {@code css.extract_only} corresponds to Jina Reader's {@code X-Target-Selector} and {@code css.remove} to {@code X-Remove-Selector}.
 * Both only take effect on inputs that Jina Reader renders as HTML; the accepted values are validated by the Elastic Inference Service.
 */
public class ElasticInferenceServiceDocumentExtractionTaskSettings implements TaskSettings {

    public static final String NAME = "elastic_inference_service_document_extraction_task_settings";
    public static final String OUTPUT_FORMAT = "output_format";
    public static final String CSS = "css";

    private static final TransportVersion INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED = TransportVersion.fromName(
        "inference_api_eis_document_extraction_added"
    );

    public static final ElasticInferenceServiceDocumentExtractionTaskSettings EMPTY_SETTINGS =
        new ElasticInferenceServiceDocumentExtractionTaskSettings(null, CssSettings.EMPTY);

    /**
     * Parses task settings from a raw config map, removing the fields it recognizes so callers can reject leftover unknown fields.
     * A null or empty map produces {@link #EMPTY_SETTINGS}.
     */
    public static ElasticInferenceServiceDocumentExtractionTaskSettings fromMap(@Nullable Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return EMPTY_SETTINGS;
        }

        ValidationException validationException = new ValidationException();

        String outputFormat = ServiceUtils.extractOptionalString(map, OUTPUT_FORMAT, SettingsScope.TASK_SETTINGS, validationException);
        var cssSettings = CssSettings.fromMap(ServiceUtils.extractOptionalMap(map, CSS, validationException), validationException);

        validationException.throwIfValidationErrorsExist();

        return new ElasticInferenceServiceDocumentExtractionTaskSettings(outputFormat, cssSettings);
    }

    /**
     * Merges stored and request task settings: a field set in {@code requestSettings} overrides the stored value, otherwise the stored
     * value is kept. The merge is per field, so a request can override {@code css.extract_only} while keeping the stored
     * {@code css.remove}.
     */
    public static ElasticInferenceServiceDocumentExtractionTaskSettings of(
        ElasticInferenceServiceDocumentExtractionTaskSettings originalSettings,
        ElasticInferenceServiceDocumentExtractionTaskSettings requestSettings
    ) {
        return new ElasticInferenceServiceDocumentExtractionTaskSettings(
            requestSettings.outputFormat != null ? requestSettings.outputFormat : originalSettings.outputFormat,
            CssSettings.of(originalSettings.css, requestSettings.css)
        );
    }

    private final String outputFormat;
    private final CssSettings css;

    public ElasticInferenceServiceDocumentExtractionTaskSettings(StreamInput in) throws IOException {
        this(in.readOptionalString(), new CssSettings(in));
    }

    public ElasticInferenceServiceDocumentExtractionTaskSettings(@Nullable String outputFormat) {
        this(outputFormat, CssSettings.EMPTY);
    }

    public ElasticInferenceServiceDocumentExtractionTaskSettings(@Nullable String outputFormat, CssSettings css) {
        this.outputFormat = outputFormat;
        this.css = Objects.requireNonNull(css);
    }

    @Nullable
    public String outputFormat() {
        return outputFormat;
    }

    public CssSettings css() {
        return css;
    }

    @Override
    public boolean isEmpty() {
        return Strings.isNullOrEmpty(outputFormat) && css.isEmpty();
    }

    @Override
    public TaskSettings updatedTaskSettings(Map<String, Object> newSettings) {
        return of(this, fromMap(newSettings));
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return INFERENCE_API_EIS_DOCUMENT_EXTRACTION_ADDED;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(outputFormat);
        css.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        toXContentFragment(builder, params);
        builder.endObject();
        return builder;
    }

    /**
     * Writes the individual settings as fields of the object the {@code builder} is currently in, without wrapping them in an object
     * of their own. The Elastic Inference Service expects the settings as top-level fields of its request body rather than nested
     * under {@code task_settings}, so the request entity uses this to inline them. Unset settings are skipped.
     */
    public XContentBuilder toXContentFragment(XContentBuilder builder, Params params) throws IOException {
        if (Strings.isNullOrEmpty(outputFormat) == false) {
            builder.field(OUTPUT_FORMAT, outputFormat);
        }
        if (css.isEmpty() == false) {
            builder.field(CSS, css);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ElasticInferenceServiceDocumentExtractionTaskSettings that = (ElasticInferenceServiceDocumentExtractionTaskSettings) o;
        return Objects.equals(outputFormat, that.outputFormat) && Objects.equals(css, that.css);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputFormat, css);
    }

    /**
     * CSS selector based content shaping. {@code extractOnly} restricts extraction to the elements matching the selectors (Jina
     * Reader's {@code X-Target-Selector}), {@code remove} drops the matching elements before extraction ({@code X-Remove-Selector}).
     * A null list means "not set", so it does not override a stored value when merging.
     */
    public record CssSettings(@Nullable List<String> extractOnly, @Nullable List<String> remove) implements ToXContentObject, Writeable {

        public static final String EXTRACT_ONLY = "extract_only";
        public static final String REMOVE = "remove";

        public static final CssSettings EMPTY = new CssSettings(null, null);

        /**
         * Parses the {@code css} object. Returns {@link #EMPTY} for a null map. The selectors are not validated beyond their type;
         * they are forwarded as-is and validated by the Elastic Inference Service.
         */
        static CssSettings fromMap(@Nullable Map<String, Object> map, ValidationException validationException) {
            if (map == null) {
                return EMPTY;
            }

            var extractOnly = InferenceUtils.extractOptionalList(map, EXTRACT_ONLY, String.class, validationException);
            var remove = InferenceUtils.extractOptionalList(map, REMOVE, String.class, validationException);

            return new CssSettings(extractOnly == null ? null : List.copyOf(extractOnly), remove == null ? null : List.copyOf(remove));
        }

        static CssSettings of(CssSettings original, CssSettings request) {
            return new CssSettings(
                request.extractOnly != null ? request.extractOnly : original.extractOnly,
                request.remove != null ? request.remove : original.remove
            );
        }

        public CssSettings(StreamInput in) throws IOException {
            this(in.readOptionalStringCollectionAsList(), in.readOptionalStringCollectionAsList());
        }

        public boolean isEmpty() {
            return extractOnly == null && remove == null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalStringCollection(extractOnly);
            out.writeOptionalStringCollection(remove);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (extractOnly != null) {
                builder.stringListField(EXTRACT_ONLY, extractOnly);
            }
            if (remove != null) {
                builder.stringListField(REMOVE, remove);
            }
            builder.endObject();
            return builder;
        }
    }
}
