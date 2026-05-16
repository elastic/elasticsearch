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
 * constant on {@link QuerySettings}; the constant carries the schema (name, type, default, parsers,
 * validator, body exposure, lifecycle flags) and is the read key. Use {@link #get(EffectiveSettings)}
 * anywhere to read the resolved value:
 *
 * <pre>{@code
 *   ZoneId tz = QuerySettings.TIME_ZONE.get(envelope);
 * }</pre>
 *
 * <p>Construct via the static factories ({@link #string}, {@link #integer}, {@link #bool},
 * {@link #enumOf}, {@link #object}) and refine with the {@code with*} fluent modifiers. Each factory
 * auto-registers the setting.
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
        return register(new QuerySettingDef<T>(name, DataType.KEYWORD)).installFromString(from);
    }

    /** An integer setting. */
    public static QuerySettingDef<Integer> integer(String name, int defaultValue) {
        return register(new QuerySettingDef<Integer>(name, DataType.INTEGER)).withDefault(defaultValue)
            .installExpressionReader(e -> ((Number) Foldables.literalValueOf(e)).intValue())
            .installJsonReader(XContentParser::intValue);
    }

    /** A boolean setting. */
    public static QuerySettingDef<Boolean> bool(String name, boolean defaultValue) {
        return register(new QuerySettingDef<Boolean>(name, DataType.BOOLEAN)).withDefault(defaultValue)
            .installExpressionReader(e -> (Boolean) Foldables.literalValueOf(e))
            .installJsonReader(XContentParser::booleanValue);
    }

    /** An enum-valued keyword setting (case-insensitive on the wire). */
    public static <E extends Enum<E>> QuerySettingDef<E> enumOf(String name, Class<E> type, E defaultValue) {
        return string(name, s -> Enum.valueOf(type, s.toUpperCase(Locale.ROOT))).withDefault(defaultValue);
    }

    /** Escape hatch for non-primitive types. Provide both a JSON parser and an expression parser. */
    public static <T> QuerySettingDef<T> object(String name, JsonReader<T> jsonReader, ExpressionReader<T> expressionReader) {
        return register(new QuerySettingDef<T>(name, null)).installJsonReader(jsonReader).installExpressionReader(expressionReader);
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

    // ---------- fluent setters (with* uniform) ----------

    /** The value to return when no source supplied one. */
    public QuerySettingDef<T> withDefault(@Nullable T value) {
        this.defaultValue = value;
        return this;
    }

    /** A value-level validator. Returns {@code null} on success or a human error message. */
    public QuerySettingDef<T> withValidator(Validator<T> validator) {
        this.validator = validator;
        return this;
    }

    /**
     * Opt in: the setting is reachable from the {@code _query} request body under {@code settings.<name>}.
     * Without this the setting is SET-only.
     */
    public QuerySettingDef<T> withRequestBody() {
        this.requestBody = true;
        return this;
    }

    /**
     * Add a body alias at the top level of the request body, named after the SET key. Implies
     * {@link #withRequestBody()}. Convenience for the common BWC case.
     */
    public QuerySettingDef<T> withAliasAtRoot() {
        return withAliasAt("", name);
    }

    /** As {@link #withAliasAtRoot()} but with a different name. */
    public QuerySettingDef<T> withAliasAtRoot(String aliasName) {
        return withAliasAt("", aliasName);
    }

    /**
     * Add a body alias at an arbitrary location. {@code parentPath} is a dotted JSON path to the parent
     * object ({@code ""} = root). Implies {@link #withRequestBody()}. May be called multiple times to add
     * multiple aliases.
     */
    public QuerySettingDef<T> withAliasAt(String parentPath, String name) {
        this.requestBody = true;
        this.aliases.add(new RequestBodyBinding(parentPath, name));
        return this;
    }

    /**
     * Define how this setting merges values arriving from different precedence sources. The default is
     * "highest non-null wins" — correct for scalars (a time zone is one time zone). Override only for
     * settings whose value is a structured object made of independent fields that should combine across
     * sources (the only current case is {@code approximation}).
     */
    public QuerySettingDef<T> withMerger(Merger<T> merger) {
        this.merger = merger;
        return this;
    }

    /** Marks this setting as a preview feature. */
    public QuerySettingDef<T> withPreview() {
        this.preview = true;
        return this;
    }

    /** Restrict to snapshot builds. */
    public QuerySettingDef<T> withSnapshotOnly() {
        this.snapshotOnly = true;
        return this;
    }

    /** Restrict to serverless deployments. */
    public QuerySettingDef<T> withServerlessOnly() {
        this.serverlessOnly = true;
        return this;
    }

    // ---------- bare-name getters ----------

    /** Reads the resolved value from the envelope. */
    public T get(EffectiveSettings settings) {
        return settings.get(this);
    }

    @Nullable
    public T defaultValue() {
        return defaultValue;
    }

    /** True if this setting accepts a value from the request body. */
    public boolean requestBody() {
        return requestBody;
    }

    public List<RequestBodyBinding> aliases() {
        return Collections.unmodifiableList(aliases);
    }

    public boolean preview() {
        return preview;
    }

    public boolean snapshotOnly() {
        return snapshotOnly;
    }

    public boolean serverlessOnly() {
        return serverlessOnly;
    }

    public Merger<T> merger() {
        return merger;
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
    private Merger<T> merger = (lower, higher) -> higher != null ? higher : lower;
    private boolean requestBody = false;
    private final List<RequestBodyBinding> aliases = new ArrayList<>();
    private boolean preview = false;
    private boolean snapshotOnly = false;
    private boolean serverlessOnly = false;

    private QuerySettingDef<T> installJsonReader(JsonReader<T> reader) {
        this.jsonReader = reader;
        return this;
    }

    private QuerySettingDef<T> installExpressionReader(ExpressionReader<T> reader) {
        this.expressionReader = reader;
        return this;
    }

    /** Convenience: derive both expression and JSON readers from a single string-to-T function. */
    private QuerySettingDef<T> installFromString(FromString<T> from) {
        return installJsonReader(p -> from.parse(p.text())).installExpressionReader(
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
    public interface Merger<T> {
        /** Combine a lower-precedence value with a higher-precedence value into the resolved value. */
        T merge(@Nullable T lower, @Nullable T higher);
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
