/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * The typed handle for one ES|QL query setting. Declared as a {@code public static final}
 * constant on {@link QuerySettings}.
 *
 * <h2>Mental model</h2>
 *
 * A setting is a typed knob. Users always supply it via in-query {@code SET}. For tooling that
 * builds requests programmatically, a setting can also be exposed in the request body. We declare
 * the knob once; the framework wires both surfaces, resolves precedence automatically
 * ({@code default < body < SET}), and gives downstream code one typed read.
 *
 * <h2>What you specify</h2>
 *
 * Required:
 * <ul>
 *   <li>a name — used as both the {@code SET} key and the body key,</li>
 *   <li>a value type — picked by choosing a factory.</li>
 * </ul>
 *
 * Optional:
 * <ul>
 *   <li>{@code withDefault} — value readers see when no source supplied one;</li>
 *   <li>{@code withRequestBody} — opt the setting into the body under {@code settings.<name>};</li>
 *   <li>{@code withAliasAtRoot} / {@code withAliasAt} — extra body paths, used only for BWC with
 *       settings whose top-level body fields predate this framework ({@code time_zone},
 *       {@code project_routing}, {@code approximation});</li>
 *   <li>{@code withValidator} — value-level check with runtime context;</li>
 *   <li>{@code withReconciler} — custom cross-source merge (see Reconciliation);</li>
 *   <li>{@code withPreview}, {@code withSnapshotOnly}, {@code withServerlessOnly} — lifecycle.</li>
 * </ul>
 *
 * Inferred: body parser wiring, {@code SET} dispatch, the precedence fold
 * ({@code default < body < SET}, applied per setting via its reconciler), the read API. The one thing
 * you write outside the declaration is adding the constant to {@link QuerySettings#ALL} to register it.
 *
 * <h2>How to declare a setting</h2>
 *
 * <ol>
 *   <li>Pick a factory matching the value type.</li>
 *   <li>Chain only the modifiers you need.</li>
 *   <li>End with {@code build()}, which validates and constructs the setting; add its constant to
 *       {@link QuerySettings#ALL} to register it.</li>
 * </ol>
 *
 * <h3>1. SET-only</h3>
 *
 * Accepted in queries as {@code SET foo='x';}. Not reachable from the body.
 *
 * <pre>{@code
 *   public static final QuerySettingDef<String> FOO = QuerySettingDef.string("foo").build();
 * }</pre>
 *
 * <h3>2. SET + body parameter</h3>
 *
 * Also accepted as {@code "settings": { "bar": "x" }}. Tooling that constructs the body without
 * splicing the query string uses this form.
 *
 * <pre>{@code
 *   public static final QuerySettingDef<String> BAR = QuerySettingDef
 *       .string("bar").withDefault("hello").withRequestBody().build();
 * }</pre>
 *
 * <h3>3. SET + body parameter + legacy top-level alias</h3>
 *
 * Same as case 2, plus accepted at the top-level body field. Reserved for BWC with the three
 * settings whose top-level fields predate this framework.
 *
 * <pre>{@code
 *   public static final QuerySettingDef<ZoneId> TIME_ZONE = QuerySettingDef
 *       .string("time_zone", s -> ZoneId.of(s).normalized())
 *       .withDefault(ZoneOffset.UTC)
 *       .withRequestBody()
 *       .withAliasAtRoot()
 *       .build();
 * }</pre>
 *
 * <h3>4. Structured value with a custom reconciler</h3>
 *
 * Use this shape when a {@code SET} and a body contribution each fill different fields and
 * you want them combined rather than last-wins.
 *
 * <pre>{@code
 *   public static final QuerySettingDef<ApproximationSettings> APPROXIMATION = QuerySettingDef
 *       .object("approximation", ApproximationSettings::fromXContent, ApproximationSettings::parse)
 *       .withRequestBody()
 *       .withAliasAtRoot()
 *       .withReconciler((prev, cur) ->
 *           new ApproximationSettings.Builder(false).merge(prev).merge(cur).build())
 *       .streamFormat((out, value) -> value.writeTo(out), ApproximationSettings::new) // object/builder must set this
 *       .build();
 * }</pre>
 *
 * <h2>Reading</h2>
 *
 * <pre>{@code
 *   ZoneId tz = QuerySettings.TIME_ZONE.get(resolved);
 * }</pre>
 *
 * <h2>Reconciliation</h2>
 *
 * The same setting can arrive from default, body, and {@code SET} in one query, so reconciliation
 * is unavoidable. The framework folds the three in ascending precedence
 * {@code default < body < SET}. The default fold is last-wins — correct for any scalar. Override
 * with {@code withReconciler} only when a structured value's fields should combine across sources
 * rather than replace.
 *
 * <h2>Factories</h2>
 *
 * {@link #string(String)}, {@link #string(String, FromString)} (function errors surface as the
 * user-visible message), {@link #bool(String)}, {@link #object(String, JsonReader, ExpressionReader)},
 * {@link #builder(String)}.
 *
 * <h2>When things go wrong</h2>
 *
 * Three places things can fail, and each behaves the way the consumer of that surface should expect.
 * <ul>
 *   <li><b>Build time.</b> {@code build()} refuses an incoherent declaration — for example, a body-exposed
 *       setting that has no JSON reader. The node won't start, so these surface at boot rather than in
 *       production.</li>
 *   <li><b>Parse time.</b> An unknown key is a typo the client can act on, so both surfaces reject it — the body
 *       path with a 400 naming the field, the {@code SET} path with a {@link org.elasticsearch.xpack.esql.parser.ParsingException}.
 *       A known-but-deprecated key (see {@link Builder#withDeprecated}) is accepted with a deprecation warning
 *       on either surface. Type mismatches throw on either path.</li>
 *   <li><b>Resolve time.</b> A {@code snapshotOnly} setting supplied on a non-snapshot build throws. If a
 *       validator was declared, it runs against the resolved value with access to cluster context (snapshot-mode
 *       flag, cross-project mode, etc).</li>
 * </ul>
 */
public final class QuerySettingDef<T> {

    public static Builder<String> string(String name) {
        return Builder.<String>of(name, DataType.KEYWORD).fromString(s -> s);
    }

    public static <T> Builder<T> string(String name, FromString<T> from) {
        return Builder.<T>of(name, DataType.KEYWORD).fromString(from);
    }

    public static Builder<Boolean> bool(String name) {
        return Builder.<Boolean>of(name, DataType.BOOLEAN).jsonReader(XContentParser::booleanValue).expressionReader(e -> {
            Object value = Foldables.literalValueOf(e);
            if (value instanceof Boolean b) {
                return b;
            }
            throw new IllegalArgumentException("Setting [" + name + "] must be a boolean, got [" + value + "]");
        }).streamFormat((out, value) -> out.writeBoolean(value), StreamInput::readBoolean);
    }

    /** Escape hatch for non-primitive types. Supply both a JSON and an expression parser. */
    public static <T> Builder<T> object(String name, JsonReader<T> jsonReader, ExpressionReader<T> expressionReader) {
        return Builder.<T>of(name, null).jsonReader(jsonReader).expressionReader(expressionReader);
    }

    /** Direct entry point for a setting whose factory above doesn't fit. */
    public static <T> Builder<T> builder(String name) {
        return Builder.of(name, null);
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
    private final Writeable.Writer<T> streamWriter;
    private final Writeable.Reader<T> streamReader;
    @Nullable
    private final Validator<T> validator;
    private final SettingReconciler<T> reconciler;
    private final boolean requestBody;
    private final List<RequestBodyBinding> aliases;
    private final boolean preview;
    private final boolean snapshotOnly;
    private final boolean serverlessOnly;
    @Nullable
    private final String deprecationMessage;
    private final UnaryOperator<T> canonicalizer;

    private QuerySettingDef(Builder<T> b) {
        this.name = b.name;
        this.type = b.type;
        this.defaultValue = b.defaultValue;
        this.jsonReader = b.jsonReader;
        this.expressionReader = b.expressionReader;
        this.streamWriter = b.streamWriter;
        this.streamReader = b.streamReader;
        this.validator = b.validator;
        this.reconciler = b.reconciler;
        this.requestBody = b.requestBody;
        this.aliases = List.copyOf(b.aliases);
        this.preview = b.preview;
        this.snapshotOnly = b.snapshotOnly;
        this.serverlessOnly = b.serverlessOnly;
        this.deprecationMessage = b.deprecationMessage;
        this.canonicalizer = b.canonicalizer;
    }

    /**
     * Coerce a value into this setting's canonical form. Applied on every write into a resolved view (the
     * resolver, {@link ResolvedSettings#withOverride}, and the {@code ResolvedSettings(StreamInput)} wire reader),
     * so a value reaches consumers in one shape no matter
     * which surface supplied it — e.g. {@code time_zone} normalizes {@code ZoneId}s so {@code "UTC"} and
     * {@code "Z"} compare equal. Defaults to identity. {@code null} passes through untouched.
     */
    public T canonicalize(@Nullable T value) {
        return value == null ? null : canonicalizer.apply(value);
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

    /**
     * A human-readable deprecation message, or {@code null} if the setting is not deprecated. A deprecated
     * setting keeps working — it is still resolved and applied — but supplying it (via {@code SET} or the
     * request body) emits this text as a deprecation warning. Deprecation is distinct from removal: a removed
     * setting drops out of the registry and is then rejected as unknown, whereas a deprecated one stays known
     * and merely warns, so clients relying on it are not broken.
     */
    @Nullable
    public String deprecationMessage() {
        return deprecationMessage;
    }

    public SettingReconciler<T> reconciler() {
        return reconciler;
    }

    public T get(ResolvedSettings settings) {
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

    /** Serialize the typed value of this setting to the stream. Used by {@link ResolvedSettings#writeTo}. */
    public void writeValue(StreamOutput out, T value) throws IOException {
        streamWriter.write(out, value);
    }

    /** Deserialize the typed value of this setting from the stream. Used by {@link ResolvedSettings#ResolvedSettings(StreamInput)}. */
    public T readValue(StreamInput in) throws IOException {
        return streamReader.read(in);
    }

    /**
     * Fluent builder for {@link QuerySettingDef}. Terminates in {@link #build()}, which validates the
     * combination of flags and constructs the immutable setting.
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
        private Writeable.Writer<T> streamWriter;
        @Nullable
        private Writeable.Reader<T> streamReader;
        @Nullable
        private Validator<T> validator;
        private SettingReconciler<T> reconciler = (previous, current) -> current != null ? current : previous;
        private boolean requestBody = false;
        private final List<RequestBodyBinding> aliases = new ArrayList<>();
        private boolean preview = false;
        private boolean snapshotOnly = false;
        private boolean serverlessOnly = false;
        @Nullable
        private String deprecationMessage = null;
        private UnaryOperator<T> canonicalizer = UnaryOperator.identity();

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

        /**
         * Body alias at a JSON path. {@code parentPath} is a dotted path to the parent object ({@code ""} = root).
         * Implies {@link #withRequestBody()}. May be called multiple times.
         * <p>
         * Only root aliases ({@code parentPath == ""}) are wired in the request parser today; declaring a non-root
         * alias makes {@code RequestXContent} fail at parser-build time. Nested-path parsing is tracked in #149283.
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

        /** Hard availability gate: supplying this setting on a non-snapshot build is rejected at validate/resolve. */
        public Builder<T> withSnapshotOnly() {
            this.snapshotOnly = true;
            return this;
        }

        /**
         * Deployment marker for serverless-only settings. Unlike {@link #withSnapshotOnly()} this is NOT a
         * parse/resolve gate — it only drives telemetry ({@link QuerySettings#applicableIn}). A serverless-only
         * setting that must be hard-rejected on a stateful cluster enforces that through its {@link #withValidator}
         * (as {@code project_routing} does via its cross-project check).
         */
        public Builder<T> withServerlessOnly() {
            this.serverlessOnly = true;
            return this;
        }

        /**
         * Mark this setting deprecated. It keeps working (still resolved and applied), but supplying it emits
         * {@code message} as a deprecation warning. Use this instead of dropping a setting from the registry
         * when you want to steer clients away from it without breaking the ones that still send it.
         */
        public Builder<T> withDeprecated(String message) {
            this.deprecationMessage = message;
            return this;
        }

        /**
         * Coerce every value of this setting into a canonical form on write (see
         * {@link QuerySettingDef#canonicalize}). Use it when equal values can be spelled differently — e.g.
         * {@code time_zone} normalizes {@code ZoneId}s — so consumers never see two forms of the same value.
         */
        public Builder<T> canonicalize(UnaryOperator<T> canonicalizer) {
            this.canonicalizer = canonicalizer;
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
            ).streamFormat((out, value) -> out.writeString(value.toString()), in -> from.parse(in.readString()));
        }

        /**
         * Wire how the setting's typed value crosses the transport boundary. Every setting needs this;
         * each factory pre-populates it for built-in types. Settings declared via {@link #object} or
         * {@link #builder} must call this explicitly before {@code build()}.
         * <p>
         * The reader is handed a plain {@link org.elasticsearch.common.io.stream.StreamInput} (the value is read
         * from its own length-delimited blob in {@code ResolvedSettings}), so a value codec must not depend on a
         * {@code BlockStreamInput} — i.e. it cannot read a {@code Block}/{@code Column}/{@code Page}.
         */
        Builder<T> streamFormat(Writeable.Writer<T> writer, Writeable.Reader<T> reader) {
            this.streamWriter = writer;
            this.streamReader = reader;
            return this;
        }

        /**
         * Validate the builder state and construct the immutable definition. The setting is not self-registered;
         * register it by adding its constant to {@link QuerySettings#ALL} (duplicate names are caught there).
         */
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
            if (streamWriter == null || streamReader == null) {
                throw new IllegalStateException(
                    "Setting [" + name + "] has no stream format — call streamFormat(writer, reader) before build()"
                );
            }
            return new QuerySettingDef<>(this);
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
