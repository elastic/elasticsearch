/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;

public class JsonExtract extends EsqlScalarFunction {
    /**
     * Internal exception used to distinguish application-level errors (path not found,
     * array index out of bounds) from JSON parser errors (malformed JSON).
     * <p>
     * This is necessary because {@link XContentParseException} extends {@link IllegalArgumentException}.
     * Without a separate exception type, we couldn't differentiate between:
     * <ul>
     *   <li>Our errors (should preserve the specific message like "path [x] does not exist")</li>
     *   <li>Parser errors (should be converted to generic "invalid JSON input")</li>
     * </ul>
     */
    private static class JsonExtractException extends Exception {
        JsonExtractException(String message) {
            super(message);
        }
    }

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "JsonExtract",
        JsonExtract::new
    );

    private final Expression jsonInput;
    private final Expression path;

    @FunctionInfo(
        returnType = "keyword",
        description = """
            Extracts a value from a JSON string using a dot-notation path expression.
            Path matching is case-sensitive (per the JSON specification).
            Returns the extracted value as a keyword string. String values are returned without
            surrounding quotes, numbers and booleans are returned as their string representation,
            and objects or arrays are returned as JSON strings. Returns `null` if either parameter
            is `null` or if the extracted JSON value is `null`. Returns `null` and emits a warning
            if the input is not valid JSON, the path does not exist, the array index is out of bounds,
            or the path attempts to traverse through a non-object/non-array value.""",
        examples = {
            @Example(file = "json_extract", tag = "json_extract"),
            @Example(file = "json_extract", tag = "json_extract_nested", description = "Extract a nested value using dot-notation:") }
    )
    public JsonExtract(
        Source source,
        @Param(
            name = "json_input",
            type = { "keyword", "text", "_source" },
            description = "A string containing valid JSON, or the `_source` field. If `null`, the function returns `null`."
        ) Expression jsonInput,
        @Param(
            name = "path",
            type = { "keyword", "text" },
            description = "A dot-notation path expression identifying the value to extract. "
                + "Use dot notation for nested fields (e.g., `user.name`) and bracket notation "
                + "for array indices (e.g., `items[0]`). If `null`, the function returns `null`."
        ) Expression path
    ) {
        super(source, Arrays.asList(jsonInput, path));
        this.jsonInput = jsonInput;
        this.path = path;
    }

    private JsonExtract(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(jsonInput);
        out.writeNamedWriteable(path);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    /**
     * Returns true if the given type is a string type or SOURCE (for _source field).
     */
    private static boolean isStringOrSource(DataType t) {
        return DataType.isString(t) || t == DataType.SOURCE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        // First parameter accepts string types or SOURCE (for _source field)
        TypeResolution resolution = isType(jsonInput, JsonExtract::isStringOrSource, sourceText(), FIRST, "keyword", "text", "_source");
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isString(path, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return jsonInput.foldable() && path.foldable();
    }

    /**
     * Parses a path string into segments, converting bracket notation to segments.
     * E.g., "orders[1].item" -> ["orders", "1", "item"]
     */
    private static String[] parsePath(String path) {
        if (path.isEmpty()) {
            return new String[0];
        }
        // Convert bracket notation to dot notation: "orders[1].item" -> "orders.1.item"
        String converted = path.replace("[", ".").replace("]", "");
        return converted.split("\\.");
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static void process(BytesRefBlock.Builder builder, BytesRef jsonInput, BytesRef path) {
        String jsonStr = jsonInput.utf8ToString();
        String pathStr = path.utf8ToString();
        String[] pathSegments = parsePath(pathStr);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, jsonStr)) {
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                throw new JsonExtractException("invalid JSON input");
            }
            extractValue(builder, parser, pathSegments, 0, pathStr);
        } catch (JsonExtractException e) {
            throw new IllegalArgumentException(e.getMessage());
        } catch (IOException | XContentParseException e) {
            // JSON parsing errors
            throw new IllegalArgumentException("invalid JSON input");
        }
    }

    /**
     * Recursively navigates the JSON stream to extract the value at the given path.
     * <p>
     * This method uses streaming parsing to avoid creating intermediate objects for paths
     * we don't need. It walks through the JSON structure one token at a time, descending
     * only into the path segments we care about and skipping everything else.
     * <p>
     * The recursion works as follows:
     * <ol>
     *   <li>If we've consumed all path segments ({@code depth == pathSegments.length}),
     *       we've reached the target value - extract it.</li>
     *   <li>If the current token is an object, iterate through its fields looking for
     *       a key matching the current segment. Skip non-matching fields entirely.</li>
     *   <li>If the current token is an array, the segment must be a numeric index.
     *       Iterate through elements, skipping until we reach the target index.</li>
     *   <li>If the current token is a scalar (string, number, boolean, null) but we
     *       still have path segments to consume, the path is invalid.</li>
     * </ol>
     *
     * @param builder      the block builder to append the extracted value to
     * @param parser       the JSON parser positioned at the current token
     * @param pathSegments the path split into segments (e.g., ["user", "address", "city"])
     * @param depth        current position in pathSegments (0-indexed)
     * @param originalPath the original path string for error messages
     */
    private static void extractValue(
        BytesRefBlock.Builder builder,
        XContentParser parser,
        String[] pathSegments,
        int depth,
        String originalPath
    ) throws IOException, JsonExtractException {
        XContentParser.Token token = parser.currentToken();

        // Base case: we've consumed all path segments, so the parser is now positioned
        // at the value we want to extract
        if (depth == pathSegments.length) {
            extractCurrentValue(builder, parser);
            return;
        }

        String segment = pathSegments[depth];

        if (token == XContentParser.Token.START_OBJECT) {
            // Current value is an object - look for a field matching the current path segment.
            // We iterate through all fields: FIELD_NAME token followed by the field's value.
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken(); // Advance from FIELD_NAME to the field's value
                    if (fieldName.equals(segment)) {
                        // Found the matching key - recurse to process the next path segment
                        extractValue(builder, parser, pathSegments, depth + 1, originalPath);
                        return;
                    } else {
                        // Not the key we want - skip this field's entire value (including nested structures)
                        parser.skipChildren();
                    }
                }
            }
            // Exhausted all fields without finding a match
            throw new JsonExtractException("path [" + originalPath + "] does not exist");

        } else if (token == XContentParser.Token.START_ARRAY) {
            // Current value is an array - the path segment must be a numeric index.
            int targetIndex;
            try {
                targetIndex = Integer.parseInt(segment);
            } catch (NumberFormatException e) {
                // Path segment isn't a number, but we're at an array - path is invalid
                throw new JsonExtractException("path [" + originalPath + "] does not exist");
            }
            if (targetIndex < 0) {
                throw new JsonExtractException("array index out of bounds");
            }

            // Iterate through array elements until we reach the target index
            int currentIndex = 0;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (currentIndex == targetIndex) {
                    // Found the target element - recurse to process the next path segment
                    extractValue(builder, parser, pathSegments, depth + 1, originalPath);
                    return;
                }
                // Not the index we want - skip this element entirely
                parser.skipChildren();
                currentIndex++;
            }
            // Reached end of array without finding the target index
            throw new JsonExtractException("array index out of bounds");

        } else {
            // Current value is a scalar (string, number, boolean, null), but we still have
            // path segments to consume. Can't traverse into a scalar value.
            throw new JsonExtractException("path [" + originalPath + "] does not exist");
        }
    }

    /**
     * Extracts the current value from the parser and appends it to the builder.
     */
    private static void extractCurrentValue(BytesRefBlock.Builder builder, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        switch (token) {
            case VALUE_STRING -> builder.appendBytesRef(new BytesRef(parser.text()));
            case VALUE_NUMBER -> builder.appendBytesRef(new BytesRef(parser.text()));
            case VALUE_BOOLEAN -> builder.appendBytesRef(new BytesRef(Boolean.toString(parser.booleanValue())));
            case VALUE_NULL -> builder.appendNull();
            case START_OBJECT, START_ARRAY -> {
                // For objects and arrays, we need to serialize them back to JSON
                // Use the parser's built-in copyCurrentStructure would require an XContentBuilder
                // Instead, we'll read the raw text - but XContentParser doesn't give us that easily
                // So we fall back to building the structure
                StringBuilder sb = new StringBuilder();
                copyCurrentStructure(sb, parser);
                builder.appendBytesRef(new BytesRef(sb.toString()));
            }
            default -> throw new IllegalArgumentException("unexpected token: " + token);
        }
    }

    /**
     * Copies the current JSON structure (object or array) to a StringBuilder.
     */
    private static void copyCurrentStructure(StringBuilder sb, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        if (token == XContentParser.Token.START_OBJECT) {
            sb.append('{');
            boolean first = true;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    if (first == false) {
                        sb.append(',');
                    }
                    first = false;
                    sb.append('"').append(escapeJson(parser.currentName())).append("\":");
                    parser.nextToken();
                    copyValue(sb, parser);
                }
            }
            sb.append('}');
        } else if (token == XContentParser.Token.START_ARRAY) {
            sb.append('[');
            boolean first = true;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                if (first == false) {
                    sb.append(',');
                }
                first = false;
                copyValue(sb, parser);
            }
            sb.append(']');
        }
    }

    /**
     * Copies the current value to a StringBuilder.
     */
    private static void copyValue(StringBuilder sb, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        switch (token) {
            case VALUE_STRING -> sb.append('"').append(escapeJson(parser.text())).append('"');
            case VALUE_NUMBER -> sb.append(parser.text());
            case VALUE_BOOLEAN -> sb.append(parser.booleanValue());
            case VALUE_NULL -> sb.append("null");
            case START_OBJECT, START_ARRAY -> copyCurrentStructure(sb, parser);
            default -> throw new IllegalArgumentException("unexpected token: " + token);
        }
    }

    /**
     * Escapes special characters in a JSON string.
     */
    private static String escapeJson(String s) {
        StringBuilder sb = null;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            String escape = switch (c) {
                case '"' -> "\\\"";
                case '\\' -> "\\\\";
                case '\b' -> "\\b";
                case '\f' -> "\\f";
                case '\n' -> "\\n";
                case '\r' -> "\\r";
                case '\t' -> "\\t";
                default -> null;
            };
            if (escape != null) {
                if (sb == null) {
                    sb = new StringBuilder(s.length() + 16);
                    sb.append(s, 0, i);
                }
                sb.append(escape);
            } else if (sb != null) {
                sb.append(c);
            }
        }
        return sb == null ? s : sb.toString();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new JsonExtract(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, JsonExtract::new, jsonInput, path);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory jsonInputExpr = toEvaluator.apply(jsonInput);
        ExpressionEvaluator.Factory pathExpr = toEvaluator.apply(path);
        return new JsonExtractEvaluator.Factory(source(), jsonInputExpr, pathExpr);
    }

    Expression jsonInput() {
        return jsonInput;
    }

    Expression path() {
        return path;
    }
}
