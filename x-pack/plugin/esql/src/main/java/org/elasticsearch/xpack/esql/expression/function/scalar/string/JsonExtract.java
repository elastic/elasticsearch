/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
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

/**
 * ES|QL scalar function that extracts a value from a JSON string using a
 * <a href="https://datatracker.ietf.org/doc/rfc9535/">JSONPath (RFC 9535)</a> subset.
 * <p>
 * Usage: {@code JSON_EXTRACT(string, path) → keyword}
 * <p>
 * The function accepts any string-typed expression (keyword, text) or the {@code _source}
 * metadata field as the JSON input, and a string path expression. It returns the extracted
 * value as a {@code keyword}:
 * <ul>
 *   <li>Strings → the value without surrounding quotes</li>
 *   <li>Numbers → their string representation ({@code "42"}, {@code "3.14"})</li>
 *   <li>Booleans → {@code "true"} or {@code "false"}</li>
 *   <li>JSON {@code null} → ES|QL {@code null} (no warning)</li>
 *   <li>Objects/arrays → serialized back to a JSON string via {@link XContentBuilder#copyCurrentStructure}</li>
 * </ul>
 * <p>
 * Path syntax is handled by {@link JsonPath}, which parses paths like {@code "user.address.city"}
 * or {@code "$.orders[0].item"} into typed segments. See {@link JsonPath} for the full syntax
 * specification.
 *
 * <h2>JSON parsing</h2>
 * Uses Elasticsearch's streaming {@link XContentParser} to walk the JSON document. Only the
 * fields along the path are examined — all other branches are skipped via
 * {@link XContentParser#skipChildren()}, avoiding unnecessary object allocation.
 *
 * <h2>Error handling</h2>
 * Both evaluators declare {@code warnExceptions = IllegalArgumentException.class}, which means
 * the generated evaluator catches {@link IllegalArgumentException}, emits it as a warning, and
 * produces {@code null} for that row. The internal {@link JsonExtractException} is used to
 * separate our application errors from parser errors — see its javadoc for details.
 *
 * <h2>Status</h2>
 * Preview / snapshot-only. Gated behind {@code FN_JSON_EXTRACT} capability.
 */
public class JsonExtract extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "JsonExtract",
        JsonExtract::new
    );

    private final Expression str;
    private final Expression path;

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
     * <p>
     * In {@link #doExtract}, we catch {@code JsonExtractException} and re-throw as
     * {@code IllegalArgumentException} with the original message preserved. We catch
     * {@code IOException} and {@code XContentParseException} separately and convert them
     * to a generic "invalid JSON input" message.
     */
    private static class JsonExtractException extends Exception {
        JsonExtractException(String message) {
            super(message);
        }
    }

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        description = """
            Extracts a value from a JSON string using a subset of
            https://datatracker.ietf.org/doc/rfc9535/[JSONPath] syntax.
            The supported path features are dot notation for nested fields
            (`user.address.city`), bracket notation for array indices
            (`items[0]`), quoted bracket notation for keys containing dots
            or special characters (`['user.name']`) or empty string keys
            (`['']`), the `$` root selector, and any combination of these
            (`store['items'][0].name`). Dots in dot notation are always
            path separators per the JSONPath specification — a JSON key
            that literally contains a dot (e.g., `"user.name"`) must be
            accessed with bracket notation (`['user.name']`). Dot notation
            and quoted bracket notation are interchangeable for simple keys
            (`a.b` and `a['b']` produce the same result). Optional whitespace
            is allowed inside brackets (`[ 0 ]` is equivalent to `[0]`).
            Path matching is case-sensitive per the JSON specification.

            The extracted value is returned as a `keyword` string: string values without
            surrounding quotes, numbers and booleans as their string representation,
            and objects or arrays as JSON strings. Returns `null` if either parameter
            is `null` or if the extracted JSON value is `null`.

            Returns `null` and emits a warning if the input is not valid JSON, the path is
            malformed, the path does not exist, the array index is out of bounds, or the
            path attempts to traverse through a non-object/non-array value.

            This function does not support wildcards (`*`), recursive descent (`..`),
            array slicing (`[0:3]`), filter expressions (`?(@.price<10)`), or negative
            array indices (`[-1]`).""",
        examples = {
            @Example(file = "json_extract", tag = "json_extract"),
            @Example(file = "json_extract", tag = "json_extract_dollar", description = """
                The `$` prefix is optional — this query produces the same result as the previous example:"""),
            @Example(
                file = "json_extract",
                tag = "json_extract_nested",
                description = "To extract a deeply nested value, use dot-notation:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_bracket",
                description = "When a key contains dots or special characters, use quoted bracket notation. "
                    + "Here `user.name` is a single key, not a nested path:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_array",
                description = "This example extracts the second item from an array of objects using bracket notation:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_object",
                description = "When the extracted value is an object or array, it is returned as a JSON string:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_top_level_array",
                description = "To extract from a top-level JSON array, use a bracket index on the root element:"
            ),
            @Example(
                file = "json_extract",
                tag = "json_extract_deep_nesting",
                description = "This example navigates through nested objects and arrays to extract a specific value:"
            ) }
    )
    public JsonExtract(
        Source source,
        @Param(
            name = "string",
            type = { "keyword", "text", "_source" },
            description = "A string containing valid JSON, or the `_source` field. If `null`, the function returns `null`."
        ) Expression str,
        @Param(
            name = "path",
            type = { "keyword", "text" },
            description = "A path expression identifying the value to extract, using a subset of "
                + "JSONPath syntax. Supports dot notation (`user.name`), bracket notation for "
                + "array indices (`items[0]`), and quoted brackets for keys with special characters "
                + "(`['user.name']`). The `$` prefix is optional. "
                + "If `null`, the function returns `null`."
        ) Expression path
    ) {
        super(source, Arrays.asList(str, path));
        this.str = str;
        this.path = path;
    }

    private JsonExtract(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class), in.readNamedWriteable(Expression.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(str);
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
     * Validates that the two parameters have compatible types.
     * <p>
     * The first parameter ({@code str}) accepts keyword, text, or _source. We use
     * {@code isType()} with a custom predicate because {@code isString()} doesn't
     * accept {@code DataType.SOURCE}. The second parameter ({@code path}) is a
     * standard string check via {@code isString()}.
     */
    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isType(
            str,
            t -> DataType.isString(t) || t == DataType.SOURCE,
            sourceText(),
            FIRST,
            "keyword",
            "text",
            "_source"
        );
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
        return str.foldable() && path.foldable();
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static void process(BytesRefBlock.Builder builder, BytesRef str, BytesRef path) {
        doExtract(builder, str, JsonPath.parse(path.utf8ToString()));
    }

    @Evaluator(extraName = "Constant", warnExceptions = IllegalArgumentException.class)
    static void processConstant(BytesRefBlock.Builder builder, BytesRef str, @Fixed JsonPath path) {
        doExtract(builder, str, path);
    }

    /**
     * Core extraction logic shared by both evaluators.
     * <p>
     * Detects the content type from the raw bytes via {@link XContentFactory#xContentType(byte[], int, int)},
     * then opens a streaming parser for that type. This handles all Elasticsearch content encodings
     * (JSON, SMILE, CBOR, YAML) — important because {@code _source} preserves the original encoding
     * the document was indexed with. For keyword/text inputs the content will always be JSON.
     * <p>
     * The error handling uses two catch blocks — see {@link JsonExtractException} for why:
     * <ul>
     *   <li>{@code JsonExtractException} — our application errors (path not found, index out of
     *       bounds). The message is preserved as-is because it's already user-friendly.</li>
     *   <li>{@code IOException | XContentParseException} — parser errors (malformed input).
     *       Converted to a generic "invalid JSON input" message because parser error details
     *       are implementation noise that wouldn't help the user.</li>
     * </ul>
     */
    private static void doExtract(BytesRefBlock.Builder builder, BytesRef str, JsonPath path) {
        // Detect the content type from the raw bytes. This handles all Elasticsearch encodings:
        // JSON (first byte '{'), SMILE (3-byte magic header), CBOR (object type markers),
        // and YAML ('---' header). Falls back to JSON for bare values like "true", "42",
        // or "[1,2]" whose first byte doesn't match any content type header — this is correct
        // because _source always starts with an object (detected reliably), while keyword/text
        // inputs are always JSON.
        XContentType type = XContentFactory.xContentType(str.bytes, str.offset, str.length);
        if (type == null) {
            type = XContentType.JSON;
        }
        try (XContentParser parser = type.xContent().createParser(XContentParserConfiguration.EMPTY, str.bytes, str.offset, str.length)) {
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                throw new JsonExtractException("invalid JSON input");
            }
            extractValue(builder, parser, path.segments(), 0, path.originalPath());
        } catch (JsonExtractException e) {
            throw new IllegalArgumentException(e.getMessage());
        } catch (IOException | XContentParseException e) {
            throw new IllegalArgumentException("invalid JSON input");
        }
    }

    /**
     * Recursively navigates the JSON stream to extract the value at the given path.
     * <p>
     * This is the core streaming traversal. The {@code depth} parameter tracks how far
     * along the segment list we've progressed. At each level, we look at the current JSON
     * token and the current path segment to decide how to navigate:
     * <ul>
     *   <li><b>Base case</b> ({@code depth == segments.size()}): we've consumed all segments,
     *       so the parser is positioned at the target value. Delegate to
     *       {@link #extractCurrentValue} to read it.</li>
     *   <li><b>Object + Key segment</b>: scan the object's fields for one matching the key name.
     *       On match, recurse with {@code depth + 1}. Non-matching fields are skipped entirely
     *       via {@code parser.skipChildren()} — this is where the streaming approach saves work.
     *       If no field matches, throw "path does not exist". For duplicate keys, the first
     *       match wins (streaming parser semantics, consistent with ClickHouse).</li>
     *   <li><b>Array + Index segment</b>: iterate through array elements counting up to the
     *       target index. On match, recurse with {@code depth + 1}. Skipped elements are
     *       discarded via {@code parser.skipChildren()}. If we reach the end of the array
     *       without finding the index, throw "array index out of bounds".</li>
     *   <li><b>Type mismatch</b> (e.g., Key segment but current token is an array, or Index
     *       segment but current token is an object, or current token is a scalar): the path
     *       cannot be followed further, so throw "path does not exist".</li>
     * </ul>
     *
     * @param depth        current position in the segments list (0 = first segment)
     * @param originalPath the path as the user wrote it, for error messages only
     */
    private static void extractValue(
        BytesRefBlock.Builder builder,
        XContentParser parser,
        List<JsonPath.Segment> segments,
        int depth,
        String originalPath
    ) throws IOException, JsonExtractException {
        XContentParser.Token token = parser.currentToken();

        // Base case: all segments consumed — extract whatever the parser is pointing at.
        if (depth == segments.size()) {
            extractCurrentValue(builder, parser);
            return;
        }

        JsonPath.Segment segment = segments.get(depth);

        if (token == XContentParser.Token.START_OBJECT && segment instanceof JsonPath.Segment.Key key) {
            // Current token is an object and the path segment is a key — navigate by field name.
            navigateObject(builder, parser, segments, depth, originalPath, key);
        } else if (token == XContentParser.Token.START_ARRAY && segment instanceof JsonPath.Segment.Index idx) {
            // Current token is an array and the path segment is an index — navigate by position.
            navigateArray(builder, parser, segments, depth, originalPath, idx);
        } else {
            // Type mismatch: trying to navigate a Key into an array, an Index into an object,
            // or any segment into a scalar value. The path cannot be followed.
            throw new JsonExtractException("path [" + originalPath + "] does not exist");
        }
    }

    /**
     * Scans object fields for one matching the key name. First match wins — duplicate keys
     * are handled by streaming parser semantics (consistent with ClickHouse). Non-matching
     * fields are skipped entirely via {@code parser.skipChildren()}.
     */
    private static void navigateObject(
        BytesRefBlock.Builder builder,
        XContentParser parser,
        List<JsonPath.Segment> segments,
        int depth,
        String originalPath,
        JsonPath.Segment.Key key
    ) throws IOException, JsonExtractException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String fieldName = parser.currentName();
                parser.nextToken(); // advance to the field's value
                if (fieldName.equals(key.name())) {
                    extractValue(builder, parser, segments, depth + 1, originalPath);
                    return;
                } else {
                    parser.skipChildren();
                }
            }
        }
        throw new JsonExtractException("path [" + originalPath + "] does not exist");
    }

    /**
     * Iterates through array elements counting up to the target index. Skipped elements
     * are discarded via {@code parser.skipChildren()}. If the end of the array is reached
     * without finding the index, throws "array index out of bounds".
     */
    private static void navigateArray(
        BytesRefBlock.Builder builder,
        XContentParser parser,
        List<JsonPath.Segment> segments,
        int depth,
        String originalPath,
        JsonPath.Segment.Index idx
    ) throws IOException, JsonExtractException {
        XContentParser.Token token;
        int currentIndex = 0;
        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (currentIndex == idx.index()) {
                extractValue(builder, parser, segments, depth + 1, originalPath);
                return;
            }
            parser.skipChildren();
            currentIndex++;
        }
        throw new JsonExtractException("array index out of bounds");
    }

    /**
     * Reads the value at the parser's current position and appends it to the builder.
     * This is the "leaf" of the extraction — called when all path segments have been consumed.
     * <p>
     * Scalars (strings, numbers, booleans) are read directly. JSON {@code null} becomes
     * ES|QL {@code null} (no warning). Objects and arrays are serialized to a compact JSON
     * string via {@link XContentBuilder#copyCurrentStructure}, which handles all XContent
     * encodings (JSON, SMILE, CBOR, YAML) and produces correct JSON output regardless of
     * input format.
     */
    private static void extractCurrentValue(BytesRefBlock.Builder builder, XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();

        switch (token) {
            case VALUE_STRING -> builder.appendBytesRef(new BytesRef(parser.text()));
            case VALUE_NUMBER -> builder.appendBytesRef(new BytesRef(parser.text()));
            case VALUE_BOOLEAN -> builder.appendBytesRef(new BytesRef(Boolean.toString(parser.booleanValue())));
            case VALUE_NULL -> builder.appendNull();
            case START_OBJECT, START_ARRAY -> {
                // TODO: Replace with zero-copy byte slicing once XContentParser exposes byte offsets.
                // See https://github.com/elastic/elasticsearch/issues/142873
                try (XContentBuilder jsonBuilder = XContentBuilder.builder(XContentType.JSON.xContent())) {
                    jsonBuilder.copyCurrentStructure(parser);
                    builder.appendBytesRef(BytesReference.bytes(jsonBuilder).toBytesRef());
                }
            }
            default -> throw new IllegalArgumentException("unexpected token: " + token);
        }
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new JsonExtract(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, JsonExtract::new, str, path);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        ExpressionEvaluator.Factory strExpr = toEvaluator.apply(str);
        if (path.foldable()) {
            JsonPath jsonPath = JsonPath.parse(((BytesRef) path.fold(toEvaluator.foldCtx())).utf8ToString());
            return new JsonExtractConstantEvaluator.Factory(source(), strExpr, jsonPath);
        }
        ExpressionEvaluator.Factory pathExpr = toEvaluator.apply(path);
        return new JsonExtractEvaluator.Factory(source(), strExpr, pathExpr);
    }

    Expression str() {
        return str;
    }

    Expression path() {
        return path;
    }
}
