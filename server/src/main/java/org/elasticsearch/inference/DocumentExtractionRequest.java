/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class handles the parsing of inputs used by the {@link TaskType#DOCUMENT_EXTRACTION} task type. The input for this task is
 * specified as a list of "content" objects, each of which specifies the {@link DataType}, {@link DataFormat} and the String value of a
 * document to extract content from. The {@code format} field is optional, and if not specified will use the default {@link DataFormat}
 * for the given {@link DataType}:
 * <pre>
 * "input": [
 *   {
 *     "content": {"type": "pdf", "format": "base64", "value": "data:application/pdf;base64,..."}
 *   }
 * ]</pre>
 *
 * @param inputs       The list of {@link InferenceString} documents to extract content from
 * @param taskSettings The map of task settings specific to this request
 */
public record DocumentExtractionRequest(List<InferenceString> inputs, Map<String, Object> taskSettings)
    implements
        Writeable,
        ToXContentFragment {

    public static final String INPUT_FIELD = "input";
    public static final String CONTENT_FIELD = "content";

    /**
     * The {@link DataType}s that can be used as document extraction inputs. Extraction works on binary document formats (e.g. PDFs) and
     * images of documents (via OCR), not on raw text.
     */
    public static final EnumSet<DataType> SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES = EnumSet.of(DataType.PDF, DataType.IMAGE);

    private static final ConstructingObjectParser<InferenceString, Void> INPUT_ITEM_PARSER = new ConstructingObjectParser<>(
        "document_extraction_input",
        args -> (InferenceString) args[0]
    );

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<DocumentExtractionRequest, Void> PARSER = new ConstructingObjectParser<>(
        DocumentExtractionRequest.class.getSimpleName(),
        args -> new DocumentExtractionRequest((List<InferenceString>) args[0], (Map<String, Object>) args[1])
    );

    static {
        INPUT_ITEM_PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseContent(parser),
            new ParseField(CONTENT_FIELD),
            ObjectParser.ValueType.OBJECT
        );

        PARSER.declareObjectArray(constructorArg(), INPUT_ITEM_PARSER::apply, new ParseField(INPUT_FIELD));
        PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parser.mapOrdered(),
            new ParseField(TASK_SETTINGS),
            ObjectParser.ValueType.OBJECT
        );
    }

    private static InferenceString parseContent(XContentParser parser) throws IOException {
        var inferenceString = InferenceString.PARSER.parse(parser, null);
        if (SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES.contains(inferenceString.dataType()) == false) {
            throw new XContentParseException(
                Strings.format(
                    "Field [%s] contains unsupported [%s] value [%s]. Supported values are %s",
                    CONTENT_FIELD,
                    InferenceString.TYPE_FIELD,
                    inferenceString.dataType(),
                    SUPPORTED_DOCUMENT_EXTRACTION_DATA_TYPES
                )
            );
        }
        return inferenceString;
    }

    public static DocumentExtractionRequest of(List<InferenceString> inputs) {
        return new DocumentExtractionRequest(inputs, null);
    }

    public DocumentExtractionRequest(List<InferenceString> inputs, @Nullable Map<String, Object> taskSettings) {
        this.inputs = inputs;
        this.taskSettings = Objects.requireNonNullElse(taskSettings, Map.of());
    }

    public DocumentExtractionRequest(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(InferenceString::new), in.readGenericMap());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(inputs);
        out.writeGenericMap(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(INPUT_FIELD);
        for (InferenceString input : inputs) {
            builder.startObject();
            builder.field(CONTENT_FIELD, input);
            builder.endObject();
        }
        builder.endArray();
        if (taskSettings.isEmpty() == false) {
            builder.field(TASK_SETTINGS, taskSettings);
        }
        return builder;
    }
}
