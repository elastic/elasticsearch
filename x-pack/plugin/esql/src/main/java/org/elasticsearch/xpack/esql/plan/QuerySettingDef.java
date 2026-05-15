/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The typed handle for one query setting. Each setting is declared once as a {@code public static final}
 * constant on {@link QuerySettings}; the constant itself is the schema (name, type, default, parsers,
 * validator, body exposure) and the read key. Use {@link #get(EffectiveSettings)} anywhere to read the
 * resolved value:
 *
 * <pre>{@code
 *   ZoneId tz = QuerySettings.TIME_ZONE.get(envelope);
 * }</pre>
 *
 * <p>Construct via the static factories ({@link #string}, {@link #integer}, {@link #bool},
 * {@link #enumOf}, {@link #object}) and refine with the fluent modifiers ({@link #defaultValue},
 * {@link #validate}, {@link #exposed}, {@link #aliasAtRoot}, {@link #combineWith}, {@link #preview},
 * {@link #snapshotOnly}, {@link #serverlessOnly}). Each factory auto-registers the setting.
 */
public final class QuerySettingDef<T> {

    // ---------- registry ----------

    private static final Map<String, QuerySettingDef<?>> REGISTRY = new ConcurrentHashMap<>();

    /** All registered settings, in no particular order. */
    public static Collection<QuerySettingDef<?>> all() {
        return Collections.unmodifiableCollection(REGISTRY.values());
    }

    /** Look up a setting by SET key name. Returns {@code null} for unknown keys. */
    @Nullable
    public static QuerySettingDef<?> lookup(String name) {
        return REGISTRY.get(name);
    }

    // ---------- factories ----------

    /** A keyword setting that holds the raw string. */
    public static QuerySettingDef<String> string(String name) {
        return QuerySettingDef.<String>string(name, s -> s);
    }

    /** A keyword setting whose value is parsed from the supplied string. */
    public static <T> QuerySettingDef<T> string(String name, FromString<T> from) {
        return register(new QuerySettingDef<T>(name, DataType.KEYWORD)).withFromString(from);
    }

    /** An integer setting. */
    public static QuerySettingDef<Integer> integer(String name, int defaultValue) {
        return register(new QuerySettingDef<Integer>(name, DataType.INTEGER)).defaultValue(defaultValue)
            .withExpressionReader(e -> ((Number) Foldables.literalValueOf(e)).intValue())
            .withJsonReader(XContentParser::intValue);
    }

    /** A boolean setting. */
    public static QuerySettingDef<Boolean> bool(String name, boolean defaultValue) {
        return register(new QuerySettingDef<Boolean>(name, DataType.BOOLEAN)).defaultValue(defaultValue)
            .withExpressionReader(e -> (Boolean) Foldables.literalValueOf(e))
            .withJsonReader(XContentParser::booleanValue);
    }

    /** An enum-valued keyword setting (case-insensitive on the wire). */
    public static <E extends Enum<E>> QuerySettingDef<E> enumOf(String name, Class<E> type, E defaultValue) {
        return string(name, s -> Enum.valueOf(type, s.toUpperCase(Locale.ROOT))).defaultValue(defaultValue);
    }

    /** Escape hatch for non-primitive types. Provide both a JSON parser and an expression parser. */
    public static <T> QuerySettingDef<T> object(String name, JsonReader<T> jsonReader, ExpressionReader<T> expressionReader) {
        return register(new QuerySettingDef<T>(name, null)).withJsonReader(jsonReader).withExpressionReader(expressionReader);
    }

    // ---------- identity ----------

    private final String name;
    @Nullable
    private final DataType type;

    private QuerySettingDef(String name, @Nullable DataType type) {
        this.name = name;
        this.type = type;
    }

    public String name() {
        return name;
    }

    /** The declared in-query type for parser-level validation; {@code null} skips datatype check. */
    @Nullable
    public DataType type() {
        return type;
    }

    // ---------- fluent modifiers ----------

    public QuerySettingDef<T> defaultValue(@Nullable T value) {
        this.defaultValue = value;
        return this;
    }

    public QuerySettingDef<T> validate(Validator<T> validator) {
        this.validator = validator;
        return this;
    }

    /** Opt in: accept this setting in the request body under {@code settings.<name>}. */
    public QuerySettingDef<T> exposed() {
        this.requestParameterExposed = true;
        return this;
    }

    /**
     * Adds a body alias for this setting at the top level of the request body, named after the SET key. Implies
     * {@link #exposed()}. Convenience for the common BWC case where a pre-existing top-level field shares its name
     * with the SET key.
     */
    public QuerySettingDef<T> aliasAtRoot() {
        return aliasAt("", name);
    }

    /** As {@link #aliasAtRoot()} but with a different name. */
    public QuerySettingDef<T> aliasAtRoot(String aliasName) {
        return aliasAt("", aliasName);
    }

    /**
     * Adds a body alias for this setting at an arbitrary location. {@code parentPath} is a dotted JSON path to the
     * parent object ({@code ""} = root); {@code name} is the field name inside that parent. Implies
     * {@link #exposed()}. A setting may carry multiple aliases — call this method multiple times.
     */
    public QuerySettingDef<T> aliasAt(String parentPath, String name) {
        this.requestParameterExposed = true;
        this.aliases.add(new RequestBodyBinding(parentPath, name));
        return this;
    }

    public QuerySettingDef<T> combineWith(Combiner<T> combiner) {
        this.combiner = combiner;
        return this;
    }

    public QuerySettingDef<T> preview() {
        this.preview = true;
        return this;
    }

    public QuerySettingDef<T> snapshotOnly() {
        this.snapshotOnly = true;
        return this;
    }

    public QuerySettingDef<T> serverlessOnly() {
        this.serverlessOnly = true;
        return this;
    }

    // ---------- reads ----------

    /** Reads the resolved value from the envelope. */
    public T get(EffectiveSettings settings) {
        return settings.get(this);
    }

    @Nullable
    public T defaultValue() {
        return defaultValue;
    }

    public boolean isRequestParameterExposed() {
        return requestParameterExposed;
    }

    public List<RequestBodyBinding> aliases() {
        return Collections.unmodifiableList(aliases);
    }

    public boolean isPreview() {
        return preview;
    }

    public boolean isSnapshotOnly() {
        return snapshotOnly;
    }

    public boolean isServerlessOnly() {
        return serverlessOnly;
    }

    public Combiner<T> combiner() {
        return combiner;
    }

    /** Parse a JSON value at the parser's current position into this setting's typed value. */
    public T readFromJson(XContentParser parser) throws IOException {
        if (jsonReader == null) {
            throw new IllegalStateException("Setting [" + name + "] is not body-exposed");
        }
        return jsonReader.read(parser);
    }

    /** Parse an in-query SET expression into this setting's typed value. */
    public T readFromExpression(Expression value) {
        if (expressionReader == null) {
            throw new IllegalStateException("Setting [" + name + "] has no expression reader");
        }
        return expressionReader.read(value);
    }

    /** @return {@code null} on success, or the validator's error message. */
    @Nullable
    public String runValidator(T value, SettingsValidationContext ctx) {
        return validator == null ? null : validator.validate(value, ctx);
    }

    // ---------- internal state ----------

    @Nullable
    private T defaultValue;
    @Nullable
    private JsonReader<T> jsonReader;
    @Nullable
    private ExpressionReader<T> expressionReader;
    @Nullable
    private Validator<T> validator;
    private Combiner<T> combiner = (lower, higher) -> higher != null ? higher : lower;
    private boolean requestParameterExposed = false;
    private final List<RequestBodyBinding> aliases = new ArrayList<>();
    private boolean preview = false;
    private boolean snapshotOnly = false;
    private boolean serverlessOnly = false;

    private QuerySettingDef<T> withJsonReader(JsonReader<T> reader) {
        this.jsonReader = reader;
        return this;
    }

    private QuerySettingDef<T> withExpressionReader(ExpressionReader<T> reader) {
        this.expressionReader = reader;
        return this;
    }

    /** Convenience: derive both expression and JSON readers from a single string-to-T function. */
    private QuerySettingDef<T> withFromString(FromString<T> from) {
        return withJsonReader(p -> from.parse(p.text())).withExpressionReader(
            e -> from.parse(Foldables.stringLiteralValueOf(e, "Unexpected value"))
        );
    }

    private static <T> QuerySettingDef<T> register(QuerySettingDef<T> def) {
        QuerySettingDef<?> prior = REGISTRY.putIfAbsent(def.name, def);
        if (prior != null) {
            throw new IllegalStateException("Duplicate query setting [" + def.name + "]");
        }
        return def;
    }

    // ---------- functional types ----------

    @FunctionalInterface
    public interface JsonReader<T> {
        T read(XContentParser parser) throws IOException;
    }

    @FunctionalInterface
    public interface ExpressionReader<T> {
        T read(Expression value);
    }

    @FunctionalInterface
    public interface Validator<T> {
        /**
         * @return {@code null} on success, an error message on failure. The {@code ctx} argument carries runtime flags
         *         (e.g. {@code crossProjectEnabled}); ignore it if the validator is value-only.
         */
        @Nullable
        String validate(T value, SettingsValidationContext ctx);
    }

    @FunctionalInterface
    public interface Combiner<T> {
        T combine(@Nullable T lower, @Nullable T higher);
    }

    @FunctionalInterface
    public interface FromString<T> {
        T parse(String value);
    }

    /**
     * A body-side alias path for a setting, outside the canonical {@code settings.{}} block.
     * {@code parentPath} is a dotted JSON path to the parent object ({@code ""} = root); {@code name}
     * is the field name inside that parent.
     */
    public record RequestBodyBinding(String parentPath, String name) {
        public RequestBodyBinding {
            if (parentPath == null) {
                throw new IllegalArgumentException("parentPath must not be null (use \"\" for root)");
            }
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("name must not be null or empty");
            }
        }

        public boolean isAtRoot() {
            return parentPath.isEmpty();
        }
    }
}
