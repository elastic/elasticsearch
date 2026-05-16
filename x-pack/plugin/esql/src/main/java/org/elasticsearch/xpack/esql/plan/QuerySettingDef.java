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
 * The typed handle for one ES|QL query setting. Immutable once built. Declared as a
 * {@code public static final} constant on {@link QuerySettings}; the constant is the schema,
 * the registration key, and the read key.
 *
 * <h2>What this is</h2>
 *
 * A query setting is a typed, named knob a user supplies two ways: in the query
 * ({@code SET time_zone='Europe/Paris';}) or in the request body
 * ({@code "settings": { "time_zone": "Europe/Paris" }}). We declare each knob once as a
 * {@code QuerySettingDef<T>} constant; the framework wires both surfaces, resolves precedence,
 * and exposes one typed read. Adding a setting is a one-line declaration. Nobody touches the
 * body parser, the SET dispatch, or {@code EsqlSession}.
 *
 * <h2>What you control, what the framework does</h2>
 *
 * You declare: name (also the body key), value type, optional default, whether the setting is
 * reachable from the body, optional aliases at other JSON paths, optional validation, optional
 * cross-source merge, and lifecycle gating (preview / snapshot-only / serverless-only).
 *
 * <p>The framework owns: body parser declarations, SET expression dispatch, precedence resolution,
 * registration, and the read API. You add no code outside the one declaration.
 *
 * <h2>How a value flows</h2>
 *
 * The body parser walks {@code settings.{}}, looks each key up in the registry, calls the
 * setting's JSON reader, and stashes the typed value on {@code EsqlQueryRequest}. The SQL parser
 * pulls {@code SET} statements and calls each setting's expression reader.
 * {@link QuerySettings#resolve} folds {@code default < body < SET} per setting using its
 * reconciler, producing an immutable {@link EffectiveSettings} envelope. Downstream code reads
 * {@code QuerySettings.FOO.get(effective)}. That's the entire flow — the same for every setting.
 *
 * <h2>Declaring one</h2>
 *
 * Pick a factory matching the value type, chain the modifiers you need, end with {@code build()}.
 * {@code build()} validates the declaration is coherent and registers the setting.
 *
 * <pre>{@code
 *   // SET-only, string-valued, no default
 *   public static final QuerySettingDef<String> FOO = QuerySettingDef.string("foo").build();
 *
 *   // Body-exposed string with a default
 *   public static final QuerySettingDef<String> BAR = QuerySettingDef
 *       .string("bar").withDefault("hello").withRequestBody().build();
 *
 *   // Parsed from string into a richer type, plus a legacy top-level alias for BWC
 *   public static final QuerySettingDef<ZoneId> TIME_ZONE = QuerySettingDef
 *       .string("time_zone", ZoneId::of)
 *       .withDefault(ZoneOffset.UTC)
 *       .withRequestBody()
 *       .withAliasAtRoot()
 *       .build();
 *
 *   // Structured value, field-level merge across sources
 *   public static final QuerySettingDef<ApproximationSettings> APPROXIMATION = QuerySettingDef
 *       .object("approximation", ApproximationSettings::fromXContent, ApproximationSettings::parse)
 *       .withRequestBody()
 *       .withAliasAtRoot()
 *       .withReconciler((prev, cur) ->
 *           new ApproximationSettings.Builder(false).merge(prev).merge(cur).build())
 *       .build();
 * }</pre>
 *
 * <h2>The factories</h2>
 *
 * Each factory binds both the JSON reader and the SET expression reader.
 * {@link #string(String)} and {@link #string(String, FromString)} cover strings (raw, or parsed
 * via a function like {@code ZoneId::of} — function errors surface as the user-visible message).
 * {@link #integer(String, int)} and {@link #bool(String, boolean)} cover primitives.
 * {@link #enumOf(String, Class, Enum)} parses an enum case-insensitively.
 * {@link #object(String, JsonReader, ExpressionReader)} is the escape hatch for non-primitives.
 * {@link #builder(String)} / {@link #builder(String, FromString)} are the generic entry points.
 *
 * <h2>The modifiers</h2>
 *
 * {@link Builder#withDefault} sets the value readers see when nobody supplied one.
 * {@link Builder#withRequestBody} opts the setting into the body surface at {@code settings.<name>}
 * — without it, the setting is SET-only. {@link Builder#withAliasAtRoot()} (and the variants)
 * declare extra body paths; we use them for the three settings ({@code time_zone},
 * {@code project_routing}, {@code approximation}) that had top-level body fields before this
 * framework existed. Aliases imply body exposure. {@link Builder#withValidator} runs a
 * {@code (value, ctx) -> errorMsg} check against the parsed value, with runtime context
 * (cross-project enabled, snapshot build) available.
 * {@link Builder#withReconciler} replaces the default last-wins merge — use it only when a
 * structured value's fields should combine across SET and body rather than replace.
 * {@link Builder#withPreview}, {@link Builder#withSnapshotOnly}, and
 * {@link Builder#withServerlessOnly} gate lifecycle.
 *
 * <h2>Precedence and merging</h2>
 *
 * Three sources fold in ascending precedence: default {@code <} body {@code <} SET. The default
 * reconciler is "higher wins, null is no-op" — correct for any scalar. A custom reconciler is
 * only needed when a structured value's fields should combine; today that's only
 * {@code approximation}, so a SET that sets {@code confidence_level} doesn't erase a body-supplied
 * {@code rows}. Reconciler arguments are {@code (previous, current)} — previous is everything
 * folded so far (lower precedence), current is this contribution (higher precedence).
 *
 * <h2>When things go wrong</h2>
 *
 * Build-time: {@code build()} rejects incoherent declarations at class init — snapshot-only and
 * serverless-only together, aliases without body exposure, body exposure without a JSON reader.
 * Parse-time: unknown keys under {@code settings.{}} reject with 400 (strict — tooling surface);
 * unknown SET keys warn and skip (forgiving — user-typed surface); type mismatches throw.
 * Resolve-time: snapshot-only on non-snapshot builds throws; the validator runs.
 *
 * <p>The framework never silently ignores a knob — every failure produces a single clear error
 * at the earliest phase that can see it.
 */
public final class QuerySettingDef<T> {

    private static final Map<String, QuerySettingDef<?>> REGISTRY = new ConcurrentHashMap<>();

    public static Collection<QuerySettingDef<?>> all() {
        return Collections.unmodifiableCollection(REGISTRY.values());
    }

    @Nullable
    public static QuerySettingDef<?> lookup(String name) {
        return REGISTRY.get(name);
    }

    public static Builder<String> string(String name) {
        return Builder.<String>of(name, DataType.KEYWORD).fromString(s -> s);
    }

    public static <T> Builder<T> string(String name, FromString<T> from) {
        return Builder.<T>of(name, DataType.KEYWORD).fromString(from);
    }

    public static Builder<Integer> integer(String name, int defaultValue) {
        return Builder.<Integer>of(name, DataType.INTEGER)
            .withDefault(defaultValue)
            .expressionReader(e -> ((Number) Foldables.literalValueOf(e)).intValue())
            .jsonReader(XContentParser::intValue);
    }

    public static Builder<Boolean> bool(String name, boolean defaultValue) {
        return Builder.<Boolean>of(name, DataType.BOOLEAN)
            .withDefault(defaultValue)
            .expressionReader(e -> (Boolean) Foldables.literalValueOf(e))
            .jsonReader(XContentParser::booleanValue);
    }

    public static <E extends Enum<E>> Builder<E> enumOf(String name, Class<E> type, E defaultValue) {
        return string(name, s -> Enum.valueOf(type, s.toUpperCase(Locale.ROOT))).withDefault(defaultValue);
    }

    /** Escape hatch for non-primitive types. Supply both a JSON and an expression parser. */
    public static <T> Builder<T> object(String name, JsonReader<T> jsonReader, ExpressionReader<T> expressionReader) {
        return Builder.<T>of(name, null).jsonReader(jsonReader).expressionReader(expressionReader);
    }

    /** Direct entry point for a setting whose factory above doesn't fit. */
    public static <T> Builder<T> builder(String name) {
        return Builder.of(name, null);
    }

    public static <T> Builder<T> builder(String name, FromString<T> from) {
        return Builder.<T>of(name, DataType.KEYWORD).fromString(from);
    }

    private final String name;
    @Nullable
    private final DataType type;
    @Nullable
    private final T defaultValue;
    @Nullable
    private final JsonReader<T> jsonReader;
    @Nullable
    private final ExpressionReader<T> expressionReader;
    @Nullable
    private final Validator<T> validator;
    private final SettingReconciler<T> reconciler;
    private final boolean requestBody;
    private final List<RequestBodyBinding> aliases;
    private final boolean preview;
    private final boolean snapshotOnly;
    private final boolean serverlessOnly;

    private QuerySettingDef(Builder<T> b) {
        this.name = b.name;
        this.type = b.type;
        this.defaultValue = b.defaultValue;
        this.jsonReader = b.jsonReader;
        this.expressionReader = b.expressionReader;
        this.validator = b.validator;
        this.reconciler = b.reconciler;
        this.requestBody = b.requestBody;
        this.aliases = List.copyOf(b.aliases);
        this.preview = b.preview;
        this.snapshotOnly = b.snapshotOnly;
        this.serverlessOnly = b.serverlessOnly;
    }

    public String name() {
        return name;
    }

    @Nullable
    public DataType type() {
        return type;
    }

    @Nullable
    public T defaultValue() {
        return defaultValue;
    }

    public boolean requestBody() {
        return requestBody;
    }

    public List<RequestBodyBinding> aliases() {
        return aliases;
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

    public SettingReconciler<T> reconciler() {
        return reconciler;
    }

    public T get(EffectiveSettings settings) {
        return settings.get(this);
    }

    public T readFromJson(XContentParser parser) throws IOException {
        if (jsonReader == null) {
            throw new IllegalStateException("Setting [" + name + "] is not body-exposed");
        }
        return jsonReader.read(parser);
    }

    public T readFromExpression(Expression value) {
        if (expressionReader == null) {
            throw new IllegalStateException("Setting [" + name + "] has no expression reader");
        }
        return expressionReader.read(value);
    }

    @Nullable
    public String runValidator(T value, SettingsValidationContext ctx) {
        return validator == null ? null : validator.validate(value, ctx);
    }

    /**
     * Fluent builder for {@link QuerySettingDef}. Terminates in {@link #build()}, which validates the
     * combination of flags and registers the resulting immutable setting in {@link #REGISTRY}.
     */
    public static final class Builder<T> {

        private final String name;
        @Nullable
        private final DataType type;
        @Nullable
        private T defaultValue;
        @Nullable
        private JsonReader<T> jsonReader;
        @Nullable
        private ExpressionReader<T> expressionReader;
        @Nullable
        private Validator<T> validator;
        private SettingReconciler<T> reconciler = (previous, current) -> current != null ? current : previous;
        private boolean requestBody = false;
        private final List<RequestBodyBinding> aliases = new ArrayList<>();
        private boolean preview = false;
        private boolean snapshotOnly = false;
        private boolean serverlessOnly = false;

        private Builder(String name, @Nullable DataType type) {
            this.name = name;
            this.type = type;
        }

        private static <T> Builder<T> of(String name, @Nullable DataType type) {
            return new Builder<>(name, type);
        }

        public Builder<T> withDefault(@Nullable T value) {
            this.defaultValue = value;
            return this;
        }

        public Builder<T> withValidator(Validator<T> validator) {
            this.validator = validator;
            return this;
        }

        /** Opt in: the setting is reachable from the {@code _query} request body under {@code settings.<name>}. */
        public Builder<T> withRequestBody() {
            this.requestBody = true;
            return this;
        }

        /** Body alias at the top level of the request body, named after the SET key. Implies {@link #withRequestBody()}. */
        public Builder<T> withAliasAtRoot() {
            return withAliasAt("", name);
        }

        public Builder<T> withAliasAtRoot(String aliasName) {
            return withAliasAt("", aliasName);
        }

        /**
         * Body alias at an arbitrary location. {@code parentPath} is a dotted JSON path to the parent
         * object ({@code ""} = root). Implies {@link #withRequestBody()}. May be called multiple times.
         */
        public Builder<T> withAliasAt(String parentPath, String aliasName) {
            this.requestBody = true;
            this.aliases.add(new RequestBodyBinding(parentPath, aliasName));
            return this;
        }

        /**
         * Custom merge function. Default is "highest-precedence non-null wins" — correct for scalars.
         * Override only for settings whose value is a multi-field object where partial contributions
         * from different sources should combine instead of overwriting.
         */
        public Builder<T> withReconciler(SettingReconciler<T> reconciler) {
            this.reconciler = reconciler;
            return this;
        }

        public Builder<T> withPreview() {
            this.preview = true;
            return this;
        }

        public Builder<T> withSnapshotOnly() {
            this.snapshotOnly = true;
            return this;
        }

        public Builder<T> withServerlessOnly() {
            this.serverlessOnly = true;
            return this;
        }

        Builder<T> jsonReader(JsonReader<T> reader) {
            this.jsonReader = reader;
            return this;
        }

        Builder<T> expressionReader(ExpressionReader<T> reader) {
            this.expressionReader = reader;
            return this;
        }

        Builder<T> fromString(FromString<T> from) {
            return jsonReader(p -> from.parse(p.text())).expressionReader(
                e -> from.parse(Foldables.stringLiteralValueOf(e, "Unexpected value"))
            );
        }

        /** Validate the builder state, construct the immutable definition, register it, and return it. */
        public QuerySettingDef<T> build() {
            if (snapshotOnly && serverlessOnly) {
                throw new IllegalStateException("Setting [" + name + "] cannot be both snapshotOnly and serverlessOnly");
            }
            if (aliases.isEmpty() == false && requestBody == false) {
                throw new IllegalStateException("Setting [" + name + "] has aliases but is not body-exposed");
            }
            if (requestBody && jsonReader == null) {
                throw new IllegalStateException("Setting [" + name + "] is body-exposed but has no JSON reader");
            }
            QuerySettingDef<T> def = new QuerySettingDef<>(this);
            QuerySettingDef<?> prior = REGISTRY.putIfAbsent(def.name, def);
            if (prior != null) {
                throw new IllegalStateException("Duplicate query setting [" + def.name + "]");
            }
            return def;
        }
    }

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
        @Nullable
        String validate(T value, SettingsValidationContext ctx);
    }

    @FunctionalInterface
    public interface SettingReconciler<T> {
        T reconcile(@Nullable T previous, @Nullable T current);
    }

    @FunctionalInterface
    public interface FromString<T> {
        T parse(String value);
    }

    /**
     * A body-side alias path for a setting outside the canonical {@code settings.{}} block.
     * {@code parentPath} is a dotted JSON path to the parent object ({@code ""} = root).
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
