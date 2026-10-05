/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.Build;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * The typed handle for one ES|QL query setting. Declared as a {@code public static final}
 * constant on {@link QuerySettings}.
 *
 * <h2>Mental model</h2>
 *
 * A setting is a typed knob. Users always supply it via in-query {@code SET}. For tooling that
 * builds requests programmatically, a setting can also be exposed in the request body, and an operator can be
 * allowed to change its default for a whole cluster. We declare the knob once; the framework wires every surface,
 * resolves precedence automatically ({@code default < cluster < body < SET}, ordered by whose decision a value is —
 * the product's, the operator's, the calling application's, the query author's), and gives downstream code one
 * typed read.
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
 *   <li>{@code withClusterDefault} — opt the setting into an operator-settable cluster-wide default at
 *       {@code esql.query.settings.<name>};</li>
 *   <li>{@code withAliasAtRoot} / {@code withAliasAt} — extra body paths, used only for BWC with
 *       settings whose top-level body fields predate this framework ({@code time_zone},
 *       {@code project_routing}, {@code approximation});</li>
 *   <li>{@code withValidator} — value-level check with runtime context;</li>
 *   <li>{@code withReconciler} — custom cross-source merge (see Reconciliation);</li>
 *   <li>{@code withPreview}, {@code withSnapshotOnly}, {@code withServerlessOnly} — lifecycle.</li>
 * </ul>
 *
 * Inferred: body parser wiring, {@code SET} dispatch, the derived cluster setting and its registration, the
 * precedence fold ({@code default < cluster < body < SET}, every layer applied through the reconciler), the read API. The one thing
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
 * The same setting can arrive from four places in one query, so reconciliation is unavoidable. The
 * framework folds them in ascending precedence {@code built-in default < cluster < body < SET},
 * ordered by whose decision the value is: the product's, the operator's, the calling application's,
 * the query author's. {@link #effectiveDefault} establishes the first two — the operator's value
 * folded onto the built-in default, or the built-in default alone when none is configured — and body
 * and {@code SET} then fold on top of that through the same reconciler.
 * The default fold is last-wins — correct for any scalar. Override
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

    /**
     * Namespace for every derived cluster setting. Plural, and mirroring the request body one-to-one: a setting
     * reachable in the body at {@code settings.time_zone} is reachable from cluster settings at
     * {@code esql.query.settings.time_zone}. A dedicated segment also keeps these keys from ever colliding with the
     * hand-written {@code esql.query.*} settings on {@link org.elasticsearch.xpack.esql.plugin.EsqlPlugin}.
     */
    public static final String CLUSTER_SETTING_PREFIX = "esql.query.settings.";

    /**
     * The context a setting's own {@link Validator} runs under when an operator writes the cluster setting — on
     * {@code PUT _cluster/settings} and on the {@code elasticsearch.yml} pass at node startup.
     * <p>
     * {@code isSnapshot} is truthful: the build type is fixed for the JVM. {@code crossProjectEnabled} is a
     * placeholder — it is read from node settings that this class has no access to at class-init — so no
     * cluster-defaultable setting may read it. {@link Builder#withClusterDefault()} enforces that structurally by
     * refusing a {@code serverlessOnly} setting, which is the marker the one such validator carries.
     */
    private static final SettingsValidationContext CLUSTER_UPDATE_CONTEXT = new SettingsValidationContext(
        false,
        Build.current().isSnapshot()
    );

    public static Builder<String> string(String name) {
        return Builder.<String>of(name, DataType.KEYWORD).fromString(s -> s);
    }

    public static <T> Builder<T> string(String name, FromString<T> from) {
        return Builder.<T>of(name, DataType.KEYWORD).fromString(from);
    }

    public static Builder<Boolean> bool(String name) {
        return Builder.<Boolean>of(name, DataType.BOOLEAN)
            .clusterParser(Booleans::parseBoolean)
            .jsonReader(XContentParser::booleanValue)
            .expressionReader(e -> {
                Object value = Foldables.literalValueOf(e);
                if (value instanceof Boolean b) {
                    return b;
                }
                throw new IllegalArgumentException("Setting [" + name + "] must be a boolean, got [" + value + "]");
            })
            .streamFormat((out, value) -> out.writeBoolean(value), StreamInput::readBoolean);
    }

    /** Escape hatch for non-primitive types. Supply both a JSON and an expression parser. */
    public static <T> Builder<T> object(String name, JsonReader<T> jsonReader, ExpressionReader<T> expressionReader) {
        return Builder.<T>of(name, null).jsonReader(jsonReader).expressionReader(expressionReader).clusterParser(raw -> {
            // A cluster setting is a string, so an object-valued setting takes its JSON as one, read with the same
            // parser the request body uses — the two surfaces then accept the same values by construction.
            try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, raw)) {
                if (parser.nextToken() == null) {
                    throw new IllegalArgumentException("Setting [" + name + "] cannot be empty");
                }
                T parsed = jsonReader.read(parser);
                if (parser.nextToken() != null) {
                    // Reject "false true" rather than silently keeping the first value.
                    throw new IllegalArgumentException("Setting [" + name + "] has trailing content after [" + raw + "]");
                }
                return parsed;
            } catch (IOException e) {
                throw new IllegalArgumentException("Setting [" + name + "] could not be parsed from [" + raw + "]", e);
            }
        });
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
    @Nullable
    private final Setting<T> clusterSetting;
    @Nullable
    private final FromString<T> clusterParser;

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
        this.clusterSetting = b.derivedClusterSetting;
        this.clusterParser = b.clusterParser;
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

    /**
     * The cluster setting backing this setting's default, or {@code null} if it was not declared with
     * {@link Builder#withClusterDefault()}. Its declared default is {@link #defaultValue()}, so
     * {@code include_defaults} reports the value queries actually get. Resolution does not read it — see
     * {@link #effectiveDefault}.
     */
    @Nullable
    public Setting<T> clusterSetting() {
        return clusterSetting;
    }

    /**
     * This setting's default as it stands on this cluster: the operator's value when one is configured and usable,
     * the registry default otherwise.
     * <p>
     * The operator's value is folded through the setting's {@link #reconciler()}, exactly as the request body and
     * {@code SET} layers are. Substituting it instead would make the same input resolve differently depending on
     * which layer supplied it — for a setting whose reconciler is not last-wins, a value meaning "off" arrives as a
     * sentinel the reconciler would have collapsed, and lands as "on".
     * <p>
     * Not {@link Setting#get(Settings)}, which validates on every read and would throw on a stored value whose verdict
     * has since changed. An operator's value is checked where the operator sees the failure, on
     * {@code PUT _cluster/settings} and at startup for {@code elasticsearch.yml}; a value that has since stopped being
     * usable falls back here rather than failing the query, and {@link #clusterValueError} reports why. That is
     * reachable only for cluster state arriving over the wire, since yml is validated at startup and recovered state
     * is archived rather than applied.
     */
    T effectiveDefault(Settings clusterState, Settings nodeSettings) {
        return operatorValue(clusterState, nodeSettings).value();
    }

    /**
     * Why this setting's configured operator value cannot be used here, or {@code null} if there is none or it is
     * usable. The counterpart to {@link #effectiveDefault}'s silent fallback: without it an operator would see their
     * configuration simply not apply.
     */
    @Nullable
    String clusterValueError(Settings clusterState, Settings nodeSettings) {
        return operatorValue(clusterState, nodeSettings).error();
    }

    /**
     * The operator's value as it will actually be used, together with the reason it cannot be — exactly one of which
     * is meaningful. {@link #effectiveDefault} and {@link #clusterValueError} are two views of this one computation
     * rather than two implementations of the same rules: a value the first silently falls back on and the second
     * calls usable is a silent fallback with no operator signal, which is the outcome the pair exists to prevent.
     * <p>
     * Every step that can reject the value is here, in the order resolution applies it: parse, fold through the
     * reconciler as any other layer would, validate, canonicalize. The validator sees the <b>folded</b> value,
     * because that is the one going into force — validating what was parsed would check something no query sees.
     */
    private OperatorValue<T> operatorValue(Settings clusterState, Settings nodeSettings) {
        String raw = rawOperatorValue(clusterState, nodeSettings);
        if (raw == null) {
            // Absent and present-but-null both read as null, and "unset" is the right reading of both.
            return new OperatorValue<>(defaultValue, null);
        }
        T folded;
        try {
            folded = reconciler.reconcile(defaultValue, clusterParser.parse(raw));
        } catch (Exception e) {
            return new OperatorValue<>(defaultValue, describe(e));
        }
        if (folded == null) {
            // The operator's value folded away to nothing — "off", for a setting where null means off. Nothing is
            // going into force, so there is nothing to validate and the registry default applies.
            return new OperatorValue<>(defaultValue, null);
        }
        if (validator != null) {
            String error;
            try {
                error = validator.validate(folded, CLUSTER_UPDATE_CONTEXT);
            } catch (Exception e) {
                return new OperatorValue<>(defaultValue, describe(e));
            }
            if (error != null) {
                return new OperatorValue<>(defaultValue, error);
            }
        }
        try {
            canonicalizer.apply(folded);
        } catch (Exception e) {
            return new OperatorValue<>(defaultValue, describe(e));
        }
        return new OperatorValue<>(folded, null);
    }

    /** The operator value in force and the reason it is not, exactly one of which is meaningful. */
    private record OperatorValue<V>(@Nullable V value, @Nullable String error) {}

    /**
     * The operator's configured value as written, or {@code null} if there is none. Cluster state wins over
     * {@code elasticsearch.yml}, the precedence {@code AbstractScopedSettings} applies when it merges the two.
     */
    @Nullable
    private String rawOperatorValue(Settings clusterState, Settings nodeSettings) {
        if (clusterSetting == null) {
            return null;
        }
        String fromClusterState = clusterState.get(clusterSetting.getKey());
        return fromClusterState != null ? fromClusterState : nodeSettings.get(clusterSetting.getKey());
    }

    /** Never null: a null message here would report "usable" for a value {@link #effectiveDefault} is falling back on. */
    private static String describe(Exception e) {
        if (e.getMessage() != null) {
            return e.getMessage();
        }
        // getSimpleName() is empty for an anonymous class, which would put us back to reporting nothing.
        String simpleName = e.getClass().getSimpleName();
        return simpleName.isEmpty() ? e.getClass().getName() : simpleName;
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
        @Nullable
        private FromString<T> clusterParser;
        private boolean clusterDefault = false;
        @Nullable
        private Setting<T> derivedClusterSetting;
        @Nullable
        private String declaredClusterDefault;

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

        /**
         * Opt this setting into a cluster-settings-backed default, registered at {@code esql.query.settings.<name>}.
         * <p>
         * This is <b>not a second default</b>. There is one default in the system — the one given to
         * {@link #withDefault} — and the derived cluster setting declares that same value as its own default so
         * {@code GET _cluster/settings?include_defaults} reports the truthful effective value. What the key adds is
         * the ability for an operator to change that default for every query on the cluster; any per-query source
         * still overrides it. The operator's value is folded through the reconciler like every other layer, so the
         * same input resolves identically whichever layer supplied it. Precedence becomes {@code default < cluster < body < SET},
         * ordered by whose
         * decision it is.
         * <p>
         * Opting in registers a key, never a value: nothing is written to cluster state, and the resolver contributes
         * a layer only when an operator actually set the key. A setting without this call has no cluster key at all
         * and resolves exactly as it did before.
         * <p>
         * Everything else is derived — the key, the type, the parser, the properties, the registration. The parser is
         * the same {@link FromString} the factory already uses for {@code SET} and the body, so a malformed value is
         * rejected on {@code PUT _cluster/settings} by the code that rejects it in a query, and the two cannot drift.
         * <p>
         * {@code build()} refuses the combination when it cannot honour it: no registry default to declare, no string
         * form to parse (the {@code object}/{@code builder} factories), or an availability marker that makes a
         * permanent public cluster key wrong ({@code snapshotOnly}) or its validator unevaluable at write time
         * ({@code serverlessOnly}).
         */
        public Builder<T> withClusterDefault() {
            this.clusterDefault = true;
            return this;
        }

        /**
         * As {@link #withClusterDefault()}, for a setting whose registry default is {@code null} because null is
         * itself meaningful — {@code approximation} uses it for "not requested". A registered cluster setting must
         * declare a default it can parse, so give it the string that means the same thing. {@code build()} checks
         * that it folds back to the registry default: an operator who set nothing and one who set this string must
         * be indistinguishable.
         */
        public Builder<T> withClusterDefault(String declaredDefault) {
            this.clusterDefault = true;
            this.declaredClusterDefault = declaredDefault;
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

        Builder<T> clusterParser(FromString<T> parser) {
            this.clusterParser = parser;
            return this;
        }

        Builder<T> fromString(FromString<T> from) {
            return clusterParser(from).jsonReader(p -> from.parse(p.text()))
                .expressionReader(e -> from.parse(Foldables.stringLiteralValueOf(e, "Unexpected value")))
                .streamFormat((out, value) -> out.writeString(value.toString()), in -> from.parse(in.readString()));
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
            if (clusterDefault) {
                derivedClusterSetting = deriveClusterSetting();
            }
            return new QuerySettingDef<>(this);
        }

        /**
         * Build the cluster setting backing this setting's default. Everything comes from what the declaration already
         * supplied: the key from the name, the parser from the factory, the declared default from {@link #withDefault},
         * the write-time check from {@link #withValidator}.
         * <p>
         * The refusals below all fire at class initialization, so an incoherent declaration stops the node from
         * starting rather than surfacing in production — the same failure class as the other {@code build()} checks.
         */
        private Setting<T> deriveClusterSetting() {
            String key = CLUSTER_SETTING_PREFIX + name;
            if (defaultValue == null && declaredClusterDefault == null) {
                throw new IllegalStateException(
                    "Setting ["
                        + name
                        + "] cannot have a cluster default: it has no registry default. The cluster setting declares the "
                        + "registry default as its own, so there must be one — add withDefault(...), or pass the string that "
                        + "means the same thing to withClusterDefault(String) if null is itself meaningful"
                );
            }
            if (clusterParser == null) {
                throw new IllegalStateException(
                    "Setting ["
                        + name
                        + "] cannot have a cluster default: its value has no string form. Only settings declared via "
                        + "string(...) or bool(...) derive a cluster setting"
                );
            }
            if (serverlessOnly) {
                throw new IllegalStateException(
                    "Setting ["
                        + name
                        + "] cannot have a cluster default: a serverlessOnly setting's validator reads environment that is "
                        + "not available when a cluster setting is written, so it cannot be checked at that surface"
                );
            }
            if (snapshotOnly) {
                throw new IllegalStateException(
                    "Setting ["
                        + name
                        + "] cannot have a cluster default: a snapshot-only setting must not register a permanent public "
                        + "cluster key that outlives its snapshot-only status"
                );
            }
            // The declared default is the registry default rendered as a string, so include_defaults reports the value
            // queries actually get. That round trip is already load-bearing for the wire format (see fromString, which
            // writes value.toString() and reads it back through the same parser); assert it here so a default that
            // cannot survive it fails at boot rather than reporting a different value than the one in force.
            String rendered = declaredClusterDefault != null ? declaredClusterDefault : defaultValue.toString();
            T reparsed;
            try {
                reparsed = clusterParser.parse(rendered);
            } catch (Exception e) {
                throw new IllegalStateException(
                    "Setting [" + name + "] cannot have a cluster default: its default [" + rendered + "] does not parse back",
                    e
                );
            }
            // Two different checks, because the two forms declare different things.
            //
            // An explicitly declared default stands in for a null registry default, and the property that matters is
            // that it is a no-op layer: an operator who set nothing and one who set this string must resolve
            // identically. So fold it and require the registry default back.
            //
            // A default rendered from the registry value only has to survive its own parser — folding it would
            // double it for a reconciler that combines rather than replaces, which is legitimate.
            T checked = declaredClusterDefault != null ? reconciler.reconcile(defaultValue, reparsed) : reparsed;
            T checkedCanonical = checked == null ? null : canonicalizer.apply(checked);
            T registryCanonical = defaultValue == null ? null : canonicalizer.apply(defaultValue);
            if (Objects.equals(checkedCanonical, registryCanonical) == false) {
                // Two different causes reach this point and they need different messages. Without a declared default
                // the value only had to survive its own parser, so a mismatch is a round-trip failure. With one, the
                // value is additionally folded onto the registry default, and a mismatch means the declared default
                // is not a no-op layer — which a round-trip check would not have caught.
                throw new IllegalStateException(
                    declaredClusterDefault != null
                        ? "Setting ["
                            + name
                            + "] cannot have a cluster default: its declared default is not a no-op layer, so folding "
                            + "it onto the registry default does not give the registry default back ["
                            + defaultValue
                            + "] -> ["
                            + rendered
                            + "] -> ["
                            + reparsed
                            + "] -> ["
                            + checked
                            + "]"
                        : "Setting ["
                            + name
                            + "] cannot have a cluster default: its default does not round-trip through its own "
                            + "parser ["
                            + defaultValue
                            + "] -> ["
                            + rendered
                            + "] -> ["
                            + reparsed
                            + "]"
                );
            }
            if (validator != null) {
                String defaultError;
                try {
                    // Validate the reparsed default, which is what Setting#get will hand the validator.
                    defaultError = validator.validate(reparsed, CLUSTER_UPDATE_CONTEXT);
                } catch (Exception e) {
                    throw new IllegalStateException(
                        "Setting [" + name + "] cannot have a cluster default: its own validator throws on its default",
                        e
                    );
                }
                if (defaultError != null) {
                    // Setting#get validates whatever it returns, including the declared default, so a default its own
                    // validator rejects would make GET _cluster/settings?include_defaults throw.
                    throw new IllegalStateException(
                        "Setting [" + name + "] cannot have a cluster default: its own validator rejects its default: " + defaultError
                    );
                }
            }
            Validator<T> declared = validator;
            // Run the setting's own validator at write time, so an operator's mistake is refused on PUT _cluster/settings
            // and at node startup for elasticsearch.yml — never as a per-query failure that the operator never sees.
            Setting.Validator<T> writeTimeValidator = value -> {
                String error;
                try {
                    error = declared == null ? null : declared.validate(value, CLUSTER_UPDATE_CONTEXT);
                } catch (Exception e) {
                    throw new IllegalArgumentException("[" + key + "] " + e.getMessage(), e);
                }
                if (error != null) {
                    throw new IllegalArgumentException("[" + key + "] " + error);
                }
            };
            List<Setting.Property> properties = new ArrayList<>();
            properties.add(Setting.Property.NodeScope);
            // Dynamic in every case: resolution reads the value once per query on the coordinator and ships the resolved
            // value onward, so an update can never tear a query in flight, and per-query variability is already the
            // framework's premise. A query setting that needed node-static behaviour would not be a query setting.
            properties.add(Setting.Property.Dynamic);
            if (deprecationMessage != null) {
                properties.add(Setting.Property.DeprecatedWarning);
            }
            return new Setting<>(key, rendered, clusterParser::parse, writeTimeValidator, properties.toArray(Setting.Property[]::new));
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
