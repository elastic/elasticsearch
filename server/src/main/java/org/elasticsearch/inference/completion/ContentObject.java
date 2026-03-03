/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.completion.ContentObject.ContentObjectType.FILE;
import static org.elasticsearch.inference.completion.ContentObject.ContentObjectType.IMAGE_URL;
import static org.elasticsearch.inference.completion.ContentObject.ContentObjectType.TEXT;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.CONTENT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.DETAIL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FILENAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FILE_DATA_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FILE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.FILE_ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.IMAGE_URL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TEXT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.URL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.extractOptionalFieldOfType;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.extractRequiredFieldOfType;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.getUnrecognizedTypeException;
import static org.elasticsearch.inference.completion.UnifiedCompletionRequestUtils.throwIfNotEmptyMap;

public abstract sealed class ContentObject implements NamedWriteable, ToXContent permits ContentObject.ContentObjectText,
    ContentObject.ContentObjectImage, ContentObject.ContentObjectFile {

    public enum ContentObjectType {
        TEXT,
        IMAGE_URL,
        FILE;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static ContentObjectType fromString(String name) {
            try {
                return valueOf(name.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw getUnrecognizedTypeException(name, CONTENT_FIELD, ContentObjectType.class);
            }
        }
    }

    final ContentObjectType type;

    ContentObject(ContentObjectType type) {
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public static ContentObject fromMap(Map<String, Object> map) {
        String typeString = extractRequiredFieldOfType(map, TYPE_FIELD, String.class, CONTENT_FIELD);
        ContentObjectType type = ContentObjectType.fromString(typeString);
        ContentObject content = switch (type) {
            case TEXT -> new ContentObjectText(extractRequiredFieldOfType(map, TEXT_FIELD, String.class, CONTENT_FIELD));
            case IMAGE_URL -> {
                Map<String, Object> imageUrlMap = extractRequiredFieldOfType(map, IMAGE_URL_FIELD, Map.class, CONTENT_FIELD);
                yield new ContentObjectImage(ContentObjectImage.ContentObjectImageUrl.fromMap(imageUrlMap));
            }
            case FILE -> {
                Map<String, Object> fileFieldsMap = extractRequiredFieldOfType(map, FILE_FIELD, Map.class, CONTENT_FIELD);
                yield new ContentObjectFile(ContentObjectFile.ContentObjectFileFields.fromMap(fileFieldsMap));
            }
        };
        throwIfNotEmptyMap(map, CONTENT_FIELD);
        return content;
    }

    public ContentObjectType type() {
        return type;
    }

    public static final class ContentObjectFile extends ContentObject {
        public static final String NAME = "content_object_file";
        private final ContentObjectFileFields fileFields;

        public ContentObjectFile(ContentObjectFileFields fileFields) {
            super(FILE);
            this.fileFields = fileFields;
        }

        public ContentObjectFile(StreamInput in) throws IOException {
            this(new ContentObjectFileFields(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED) == false) {
                throw new ElasticsearchStatusException(
                    "Cannot send a multimodal chat completion request to an older node. "
                        + "Please wait until all nodes are upgraded before using multimodal chat completion inputs",
                    RestStatus.BAD_REQUEST
                );
            }
            fileFields.writeTo(out);
        }

        public String toString() {
            return fileFields + ":" + type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(FILE_FIELD, fileFields);
            builder.field(TYPE_FIELD, type.toString());
            return builder.endObject();
        }

        public ContentObjectFileFields fileFields() {
            return fileFields;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ContentObjectFile) obj;
            return Objects.equals(this.fileFields, that.fileFields);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileFields);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        /**
         * Represents the fields of a file content object in a unified completion request.
         * Includes the file data, file ID, and filename.
         * The file_id field is not currently supported, but is part of the OpenAI schema,
         * so it is treated as optional for serialization.
         * Since file_id is not supported, file_data and filename are effectively required,
         * even though they are optional in the OpenAI schema.
         * They are also treated as optional for serialization, to allow future support for file_id
         * without needing a new transport version or worrying about backward compatibility.
         * @param fileData the data of the file
         * @param fileId the ID of the file
         * @param filename the name of the file
         */
        public record ContentObjectFileFields(@Nullable String fileData, @Nullable String fileId, @Nullable String filename)
            implements
                Writeable,
                ToXContentObject {

            public static ContentObjectFileFields fromMap(Map<String, Object> map) {
                String fileData = extractRequiredFieldOfType(map, FILE_DATA_FIELD, String.class, FILE_FIELD);
                String filename = extractRequiredFieldOfType(map, FILENAME_FIELD, String.class, FILE_FIELD);
                if (map.containsKey(FILE_ID_FIELD)) {
                    throw new ElasticsearchStatusException(
                        Strings.format(
                            "Field [%s] is not supported for content of type [%s]",
                            FILE_ID_FIELD,
                            ContentObject.ContentObjectType.FILE
                        ),
                        RestStatus.BAD_REQUEST
                    );
                }
                throwIfNotEmptyMap(map, FILE_FIELD);
                return new ContentObjectFileFields(fileData, null, filename);
            }

            public ContentObjectFileFields(StreamInput in) throws IOException {
                this(in.readOptionalString(), in.readOptionalString(), in.readOptionalString());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeOptionalString(fileData);
                out.writeOptionalString(fileId);
                out.writeOptionalString(filename);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                if (fileData != null) {
                    builder.field(FILE_DATA_FIELD, fileData);
                }
                if (fileId != null) {
                    builder.field(FILE_ID_FIELD, fileId);
                }
                if (filename != null) {
                    builder.field(FILENAME_FIELD, filename);
                }
                return builder.endObject();
            }
        }
    }

    public static final class ContentObjectImage extends ContentObject {
        public static final String NAME = "content_object_image";
        private final ContentObjectImageUrl imageUrl;

        public ContentObjectImage(ContentObjectImageUrl imageUrl) {
            super(IMAGE_URL);
            this.imageUrl = imageUrl;
        }

        public ContentObjectImage(StreamInput in) throws IOException {
            this(new ContentObjectImageUrl(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED) == false) {
                throw new ElasticsearchStatusException(
                    "Cannot send a multimodal chat completion request to an older node. "
                        + "Please wait until all nodes are upgraded before using multimodal chat completion inputs",
                    RestStatus.BAD_REQUEST
                );
            }
            imageUrl.writeTo(out);
        }

        public String toString() {
            return imageUrl + ":" + type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(IMAGE_URL_FIELD, imageUrl);
            builder.field(TYPE_FIELD, type.toString());
            return builder.endObject();
        }

        public ContentObjectImageUrl imageUrl() {
            return imageUrl;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ContentObjectImage) obj;
            return Objects.equals(this.imageUrl, that.imageUrl);
        }

        @Override
        public int hashCode() {
            return Objects.hash(imageUrl);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        public record ContentObjectImageUrl(String url, @Nullable ImageUrlDetail detail) implements Writeable, ToXContentObject {
            public static ContentObjectImageUrl fromMap(Map<String, Object> map) {
                String url = extractRequiredFieldOfType(map, URL_FIELD, String.class, URL_FIELD);
                String detailString = extractOptionalFieldOfType(map, DETAIL_FIELD, String.class, URL_FIELD);
                ImageUrlDetail detail = detailString == null ? null : ImageUrlDetail.fromString(detailString);
                throwIfNotEmptyMap(map, IMAGE_URL_FIELD);
                return new ContentObjectImageUrl(url, detail);
            }

            public ContentObjectImageUrl(StreamInput in) throws IOException {
                this(in.readString(), in.readOptionalEnum(ImageUrlDetail.class));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(url);
                out.writeOptionalEnum(detail);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(URL_FIELD, url);
                if (detail != null) {
                    builder.field(DETAIL_FIELD, detail);
                }
                return builder.endObject();
            }

            public enum ImageUrlDetail {
                AUTO,
                LOW,
                HIGH;

                private static final String URL_FIELD = "url";

                @Override
                public String toString() {
                    return name().toLowerCase(Locale.ROOT);
                }

                public static ImageUrlDetail fromString(String name) {
                    try {
                        return valueOf(name.trim().toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException ex) {
                        throw getUnrecognizedTypeException(name, URL_FIELD, ImageUrlDetail.class);
                    }
                }
            }
        }
    }

    public static final class ContentObjectText extends ContentObject {
        public static final String NAME = "content_object_text";

        private final String text;

        public ContentObjectText(String text) {
            super(TEXT);
            this.text = text;
        }

        public ContentObjectText(StreamInput in) throws IOException {
            this(in.readString());
            if (in.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED) == false) {
                in.readString();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTIMODAL_CHAT_COMPLETION_SUPPORT_ADDED)) {
                out.writeString(text);
            } else {
                out.writeString(text);
                out.writeString(TEXT.toString());
            }
        }

        public String toString() {
            return text + ":" + type;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TEXT_FIELD, text);
            builder.field(TYPE_FIELD, type.toString());
            return builder.endObject();
        }

        public String text() {
            return text;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null || obj.getClass() != this.getClass()) return false;
            var that = (ContentObjectText) obj;
            return Objects.equals(this.text, that.text);
        }

        @Override
        public int hashCode() {
            return Objects.hash(text);
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }
    }
}
