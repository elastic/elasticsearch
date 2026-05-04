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
import org.elasticsearch.common.xcontent.XContentParserUtils;
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

import static java.util.Collections.singletonList;
import static org.elasticsearch.inference.InferenceString.TYPE_FIELD;
import static org.elasticsearch.inference.ModelConfigurations.TASK_SETTINGS;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * This class handles the parsing of inputs used by the {@link TaskType#RERANK} task type. The {@code "input"} field for this task is
 * specified using either a single string, a list of strings, a single object, or a list of objects. When using objects, each specifies the
 * {@link DataType}, {@link DataFormat} and the String value of the input. The {@code format} field is optional, and if not specified will
 * use the default {@link DataFormat} for the given {@link DataType}.
 * <br>
 * <b>Note:</b> The ability to specify only a single input is included to maintain backwards compatibility with the
 * {@code InferenceAction.Request} parser, from which this new rerank parser was extracted, despite it being nonsensical for rerank
 * operations.
 * <br>
 * String input:
 * <pre>
 * "input": "input one"</pre>
 * OR
 * <pre>
 * "input": ["input one", "input two", "input three"]</pre>
 * Object input:
 * <pre>
 * "input": {"type": "image", "format": "base64", "value": "data:image/png;base64,..."}</pre>
 * OR
 * <pre>
 * "input": [
 *   {"type": "image", "format": "base64", "value": "data:image/png;base64,..."},
 *   {"type": "text", "value": "text input"},
 *   {"type": "image", "value": "data:image/png;base64,..."}
 * ]</pre>
 * The {@code "query"} field is specified using either a single string or a single object.
 * String query:
 * <pre>
 * "query": "some query text"</pre>
 * Object query:
 * <pre>
 * "query": {"type": "image", "format": "base64", "value": "data:image/png;base64,..."}</pre>
 *
 * @param inputs          The list of {@link InferenceString} inputs to rerank
 * @param query           The query to use when reranking inputs
 * @param topN            The number of results to return
 * @param returnDocuments Whether the request should return the input values as part of the results
 * @param taskSettings    The map of task settings specific to this request
 */
public record RerankRequest(
    List<InferenceString> inputs,
    InferenceString query,
    Integer topN,
    Boolean returnDocuments,
    Map<String, Object> taskSettings
) implements Writeable, ToXContentFragment {

    public static final String INPUT_FIELD = "input";
    public static final String QUERY_FIELD = "query";
    public static final String TOP_N_FIELD = "top_n";
    public static final String RETURN_DOCUMENTS_FIELD = "return_documents";

    // TODO: When multimodal rerank support is added, add DataType.IMAGE to this set
    public static final EnumSet<DataType> SUPPORTED_RERANK_DATA_TYPES = EnumSet.of(DataType.TEXT);

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<RerankRequest, Void> PARSER = new ConstructingObjectParser<>(
        RerankRequest.class.getSimpleName(),
        args -> new RerankRequest(
            (List<InferenceString>) args[0],
            (InferenceString) args[1],
            (Integer) args[2],
            (Boolean) args[3],
            (Map<String, Object>) args[4]
        )
    );

    static {
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseInput(parser),
            new ParseField(INPUT_FIELD),
            ObjectParser.ValueType.OBJECT_ARRAY_OR_STRING
        );
        PARSER.declareField(
            constructorArg(),
            (parser, context) -> parseStringOrObject(parser, QUERY_FIELD),
            new ParseField(QUERY_FIELD),
            ObjectParser.ValueType.OBJECT_OR_STRING
        );
        PARSER.declareInt(optionalConstructorArg(), new ParseField(TOP_N_FIELD));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField(RETURN_DOCUMENTS_FIELD));
        PARSER.declareField(
            optionalConstructorArg(),
            (parser, context) -> parser.mapOrdered(),
            new ParseField(TASK_SETTINGS),
            ObjectParser.ValueType.OBJECT
        );
    }

    public RerankRequest(
        List<InferenceString> inputs,
        InferenceString query,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        @Nullable Map<String, Object> taskSettings
    ) {
        this.inputs = inputs;
        this.query = query;
        this.topN = topN;
        this.returnDocuments = returnDocuments;
        this.taskSettings = Objects.requireNonNullElse(taskSettings, Map.of());
    }

    public RerankRequest(StreamInput in) throws IOException {
        this(
            in.readCollectionAsImmutableList(InferenceString::new),
            new InferenceString(in),
            in.readOptionalVInt(),
            in.readOptionalBoolean(),
            in.readGenericMap()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(inputs);
        query.writeTo(out);
        out.writeOptionalVInt(topN);
        out.writeOptionalBoolean(returnDocuments);
        out.writeGenericMap(taskSettings);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INPUT_FIELD, inputs);
        builder.field(QUERY_FIELD, query);
        if (topN != null) {
            builder.field(TOP_N_FIELD, topN);
        }
        if (returnDocuments != null) {
            builder.field(RETURN_DOCUMENTS_FIELD, returnDocuments);
        }
        if (taskSettings.isEmpty() == false) {
            builder.field(TASK_SETTINGS, taskSettings);
        }
        return builder;
    }

    private static List<InferenceString> parseInput(XContentParser parser) throws IOException {
        var token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING || token == XContentParser.Token.START_OBJECT) {
            // Single input of String or object
            return singletonList(parseStringOrObject(parser, INPUT_FIELD));
        } else if (token == XContentParser.Token.START_ARRAY) {
            // Array of String or objects
            return XContentParserUtils.parseList(parser, p -> parseStringOrObject(p, INPUT_FIELD));
        }
        throw new XContentParseException("Unsupported token [" + token + "]");
    }

    private static InferenceString parseStringOrObject(XContentParser parser, String fieldName) throws IOException {
        var currentToken = parser.currentToken();
        if (currentToken == XContentParser.Token.VALUE_STRING) {
            return new InferenceString(DataType.TEXT, parser.text());
        } else if (currentToken == XContentParser.Token.START_OBJECT) {
            var inferenceString = InferenceString.PARSER.parse(parser, null);
            if (SUPPORTED_RERANK_DATA_TYPES.contains(inferenceString.dataType()) == false) {
                throw new XContentParseException(
                    Strings.format(
                        "Field [%s] contains unsupported [%s] value [%s]. Supported values are %s",
                        fieldName,
                        TYPE_FIELD,
                        inferenceString.dataType(),
                        SUPPORTED_RERANK_DATA_TYPES
                    )
                );
            }
            return inferenceString;
        } else {
            throw new XContentParseException("Unsupported token [" + currentToken + "]");
        }
    }
}
