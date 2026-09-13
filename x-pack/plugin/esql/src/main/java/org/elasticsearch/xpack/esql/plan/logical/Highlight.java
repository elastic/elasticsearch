/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightSupport;
import org.elasticsearch.xpack.esql.planner.HighlightQueryBuilders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

public class Highlight extends UnaryPlan
    implements
        TelemetryAware,
        GeneratingPlan<Highlight>,
        PostAnalysisVerificationAware,
        DocPreserving {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Highlight",
        Highlight::new
    );

    /** Minimum transport version that knows how to deserialize this plan node. */
    public static final TransportVersion ESQL_HIGHLIGHT = TransportVersion.fromName("esql_highlight");

    /**
     * Older peers only know explicit {@code HIGHLIGHT <query> ON <fields>}; they do not serialize
     * {@link #implicitQuery} or {@link #derivedFields}.
     */
    public static final TransportVersion ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS = TransportVersion.fromName(
        "esql_highlight_implicit_query_and_fields"
    );

    public static final String DEFAULT_PREFIX = "highlight_";

    public static final String PRE_TAGS = "pre_tags";
    public static final String POST_TAGS = "post_tags";
    public static final String NUMBER_OF_FRAGMENTS = "number_of_fragments";
    public static final String FRAGMENT_SIZE = "fragment_size";
    public static final String ENCODER = "encoder";
    public static final String ANALYZER = "analyzer";
    public static final String NO_MATCH_SIZE = "no_match_size";
    public static final String BOUNDARY_SCANNER = "boundary_scanner";
    public static final String BOUNDARY_SCANNER_LOCALE = "boundary_scanner_locale";
    public static final String ORDER = "order";
    public static final String MAX_ANALYZED_OFFSET = "max_analyzed_offset";

    private static final List<String> VALID_OPTION_NAMES = List.of(
        PRE_TAGS,
        POST_TAGS,
        NUMBER_OF_FRAGMENTS,
        FRAGMENT_SIZE,
        ENCODER,
        ANALYZER,
        BOUNDARY_SCANNER,
        BOUNDARY_SCANNER_LOCALE,
        ORDER,
        NO_MATCH_SIZE,
        MAX_ANALYZED_OFFSET
    );

    private final String prefix;
    private final Expression query;
    /** True once analysis has borrowed the query from an upstream full-text WHERE. False at parse time. */
    private final boolean implicitQuery;
    /**
     * True when ON was omitted or is {@code *}. Set at parse time and kept after the field list is filled in.
     */
    private final boolean derivedFields;

    /**
     * Source of the effective {@code analyzer} option. This is analysis-only.
     * {@link #postAnalysisVerification(AnalysisRegistry, Failures)} uses it to pick the error for an unresolved
     * analyzer. A user-written name keeps the raw failure. A name copied from WHERE uses the borrowed-from-WHERE
     * framing. The field is not serialized and is not part of {@link #equals}. It does not affect execution. It
     * only changes the coordinator-side error message.
     * <p>
     * This is a two-value enum instead of a third constructor {@code boolean}. {@code EsqlNodeSubclassTests} builds
     * each node by drawing a value for every constructor argument that differs from all earlier arguments. A third
     * boolean can never be distinct from the other two, so generation spins until the suite times out. A two-value
     * enum does not collide with the remaining booleans, so generation finishes.
     */
    public enum AnalyzerOrigin {
        /** The {@code analyzer} option was written by the user in {@code WITH}, or is absent. */
        COMMAND,
        /** {@link org.elasticsearch.xpack.esql.analysis.rules.ResolveHighlight} copied it from the borrowed WHERE. */
        WHERE
    }

    private final AnalyzerOrigin analyzerOrigin;
    private final List<NamedExpression> fields;
    private final MapExpression options;
    /**
     * The generated attributes for the highlighted fields.
     * These are appended to the child's output in the same order as the ON fields,
     * so the operator's appended blocks line up with these layout channels.
     */
    private final List<Attribute> generatedFields;

    public Highlight(
        Source source,
        LogicalPlan child,
        String prefix,
        Expression query,
        boolean implicitQuery,
        boolean derivedFields,
        AnalyzerOrigin analyzerOrigin,
        List<NamedExpression> fields,
        MapExpression options,
        List<Attribute> generatedFields
    ) {
        super(source, child);
        this.prefix = prefix;
        this.query = query;
        this.implicitQuery = implicitQuery;
        this.derivedFields = derivedFields;
        this.analyzerOrigin = analyzerOrigin;
        this.fields = fields;
        this.options = options;
        this.generatedFields = generatedFields;
    }

    private Highlight(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readString(),
            in.readOptionalNamedWriteable(Expression.class),
            in.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS) ? in.readBoolean() : false,
            in.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS) ? in.readBoolean() : false,
            // Analyzer origin is analysis-only and is not serialized. Peers do not re-verify.
            AnalyzerOrigin.COMMAND,
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            // MapExpression is registered under the Expression category, not its own, so read it as an Expression.
            (MapExpression) in.readOptionalNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(child());
        out.writeString(prefix);
        out.writeOptionalNamedWriteable(query);
        if (out.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS)) {
            out.writeBoolean(implicitQuery);
            out.writeBoolean(derivedFields);
        } else if (implicitQuery || derivedFields) {
            // Dropping the flags would make a derived query look explicit on the peer, changing ON-list verification.
            throw new IllegalArgumentException(
                "HIGHLIGHT with a derived query or field list is not supported in peer node's version ["
                    + out.getTransportVersion()
                    + "]. Upgrade to version ["
                    + ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS
                    + "] or newer."
            );
        }
        out.writeNamedWriteableCollection(fields);
        out.writeOptionalNamedWriteable(options);
        out.writeNamedWriteableCollection(generatedFields);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String prefix() {
        return prefix;
    }

    public Expression query() {
        return query;
    }

    public boolean implicitQuery() {
        return implicitQuery;
    }

    public boolean derivedFields() {
        return derivedFields;
    }

    public AnalyzerOrigin analyzerOrigin() {
        return analyzerOrigin;
    }

    public List<NamedExpression> fields() {
        return fields;
    }

    public MapExpression options() {
        return options;
    }

    public static List<String> validOptionNames() {
        return VALID_OPTION_NAMES;
    }

    /**
     * Builds nullable {@code <prefix><field>} keyword attributes for the given highlight fields.
     * A generated name that collides with existing output shadows the earlier column.
     */
    public static List<Attribute> generatedAttributesFor(Source source, String prefix, List<NamedExpression> fields) {
        return fields.stream()
            .map(f -> (Attribute) new ReferenceAttribute(source, null, prefix + f.name(), DataType.KEYWORD, Nullability.TRUE, null, false))
            .toList();
    }

    private Highlight copy(
        LogicalPlan child,
        Expression query,
        List<NamedExpression> fields,
        MapExpression options,
        List<Attribute> generatedFields
    ) {
        return new Highlight(
            source(),
            child,
            prefix,
            query,
            implicitQuery,
            derivedFields,
            analyzerOrigin,
            fields,
            options,
            generatedFields
        );
    }

    public Highlight withOptions(MapExpression newOptions) {
        if (Objects.equals(options, newOptions)) {
            return this;
        }
        return copy(child(), query, fields, newOptions, generatedFields);
    }

    /**
     * Keeps {@link #derivedFields} while replacing the rest: verification reads it to skip ON-membership enforcement
     * for a field list the user never wrote. Pass {@code newGeneratedFields} through unchanged unless
     * {@code newFields} changed, since {@code generatedAttributesFor} mints fresh {@link NameId}s on every call.
     */
    public Highlight withResolved(
        Expression newQuery,
        boolean newImplicitQuery,
        AnalyzerOrigin newAnalyzerOrigin,
        List<NamedExpression> newFields,
        List<Attribute> newGeneratedFields,
        MapExpression newOptions
    ) {
        return new Highlight(
            source(),
            child(),
            prefix,
            newQuery,
            newImplicitQuery,
            derivedFields,
            newAnalyzerOrigin,
            newFields,
            newOptions,
            newGeneratedFields
        );
    }

    /**
     * Narrows this HIGHLIGHT to a subset of its ON fields and their aligned generated columns, keeping {@link #query},
     * {@link #implicitQuery} and {@link #derivedFields}. Column pruning uses this to drop ON fields whose generated
     * {@code <prefix><field>} column is never consumed downstream, except fields the query still translates against.
     * This runs after verification, so ON-membership is unaffected; a literal query simply spans fewer fields, but a
     * {@code QSTR} or {@code MATCH} that names a pruned field would fail at runtime.
     */
    public Highlight withPrunedFields(List<NamedExpression> prunedFields, List<Attribute> prunedGeneratedFields) {
        return copy(child(), query, prunedFields, options, prunedGeneratedFields);
    }

    @Override
    public Highlight replaceChild(LogicalPlan newChild) {
        return copy(newChild, query, fields, options, generatedFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            Highlight::new,
            child(),
            prefix,
            query,
            implicitQuery,
            derivedFields,
            analyzerOrigin,
            fields,
            options,
            generatedFields
        );
    }

    @Override
    public List<Attribute> output() {
        return mergeOutputAttributes(generatedFields, child().output());
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return generatedFields;
    }

    @Override
    public Highlight withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        List<Attribute> renamed = new ArrayList<>(generatedFields.size());
        for (int i = 0; i < generatedFields.size(); i++) {
            Attribute attr = generatedFields.get(i);
            String newName = newNames.get(i);
            renamed.add(newName.equals(attr.name()) ? attr : attr.withName(newName).withId(new NameId()));
        }
        return copy(child(), query, fields, options, renamed);
    }

    @Override
    protected AttributeSet computeReferences() {
        // Only the ON fields are inputs; the generated <prefix><field> columns are outputs, not references.
        return Expressions.references(fields);
    }

    @Override
    public boolean expressionsResolved() {
        // No ON list (bare HIGHLIGHT / HIGHLIGHT <query>): analysis still has to derive fields,
        // and until then output() has no generated columns. ON * is [UnresolvedStar] and is
        // rejected by the loop below, not by isEmpty().
        if (fields.isEmpty() || (query != null && query.resolved() == false)) {
            return false;
        }
        for (NamedExpression field : fields) {
            if (field.resolved() == false) {
                return false;
            }
        }
        return options == null || options.resolved();
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        verifyFieldTypes(failures);
        if (query == null) {
            // ResolveHighlight stores the borrowed query, not why borrowing failed, so recompute the reason here.
            failures.add(fail(this, "{}", HighlightSupport.collectImplicitQuery(child(), source()).reasonIfMissing()));
        } else if (fields.isEmpty()) {
            failures.add(fail(this, "{}", HighlightSupport.noHighlightableFieldsMessage(query)));
        } else if (implicitQuery && query.resolved()) {
            String mismatch = HighlightSupport.implicitQueryFieldMismatchMessage(query, fields);
            if (mismatch != null) {
                failures.add(fail(this, "{}", mismatch));
            }
        }
        if (options == null) {
            return;
        }
        verifyEnum(failures, HighlightOptions.ENCODER_OPTION);
        verifyEnum(failures, HighlightOptions.BOUNDARY_SCANNER_OPTION);
        verifyEnum(failures, HighlightOptions.ORDER_OPTION);
        for (String name : VALID_OPTION_NAMES) {
            verifyValue(failures, name);
        }
    }

    @Override
    public void postAnalysisVerification(AnalysisRegistry analysisRegistry, Failures failures) {
        postAnalysisVerification(failures);
        if (query == null || query.resolved() == false || fields.isEmpty()) {
            return;
        }
        String commandAnalyzerName;
        try {
            commandAnalyzerName = analyzerOptionName();
        } catch (IllegalArgumentException e) {
            // The analyzer value isn't a string. Type errors have already been reported by verifyValue, but still
            // validate the query with the default per-field analyzers so query errors are surfaced too.
            commandAnalyzerName = null;
        }
        List<String> fieldNames = fields.stream().map(NamedExpression::name).toList();
        Map<String, String> fieldAnalyzerNames;
        try {
            fieldAnalyzerNames = HighlightSupport.fieldAnalyzers(query, commandAnalyzerName, fieldNames);
        } catch (IllegalArgumentException e) {
            failures.add(fail(this, "{}", e.getMessage()));
            return;
        }
        verifyQuery(fieldAnalyzerNames, commandAnalyzerName, failures, analysisRegistry);
    }

    /** The user-set {@code WITH {"analyzer": ...}} name, or {@code null} when absent. */
    private String analyzerOptionName() {
        Expression value = options == null ? null : foldableOption(ANALYZER);
        return value == null ? null : HighlightOptions.analyzerName(ANALYZER, value, FoldContext.small());
    }

    /**
     * Message when a borrowed WHERE analyzer is not a registered analyzer. Covers ON-field primaries, off-ON leaf
     * analyzers, and {@code quote_analyzer}.
     */
    private static String borrowedUnresolvedAnalyzerMessage(String name) {
        return "HIGHLIGHT derived its query from a preceding WHERE, but that query refers to analyzer ["
            + name
            + "], which is not a registered analyzer. Per-index custom analyzers cannot be used in HIGHLIGHT. "
            + "Provide an explicit HIGHLIGHT query that does not use analyzer ["
            + name
            + "].";
    }

    private void verifyQuery(
        Map<String, String> fieldAnalyzerNames,
        String commandAnalyzerName,
        Failures failures,
        AnalysisRegistry analysisRegistry
    ) {
        if (verifyAnalyzerNames(fieldAnalyzerNames, commandAnalyzerName, failures, analysisRegistry)) {
            return;
        }
        try {
            // ON membership is enforced only when the user wrote both the query and the field list. A borrowed
            // query translates leniently so a predicate naming a non-ON field becomes match-none rather than failing.
            HighlightQueryBuilders.verify(
                query,
                HighlightQueryBuilders.resolveFieldAnalyzers(fieldAnalyzerNames, analysisRegistry),
                implicitQuery == false && derivedFields == false,
                implicitQuery,
                analysisRegistry
            );
        } catch (IllegalArgumentException e) {
            // Attach to the query node, not this Highlight node: failures dedupe by node, so pinning it here would let a
            // co-located option/analyzer failure on this node swallow the query error (see VerifierTests#testHighlightAnalyzerOption).
            failures.add(fail(query, "{}", e.getMessage()));
        }
    }

    /**
     * Resolves every analyzer name this command needs, one name at a time, so a failure is reported with the offending
     * name in hand rather than recovered from another class's exception text. Returns {@code true} when a failure was
     * recorded, in which case the query is not verified further: with an analyzer missing, any query error it then
     * raises is a consequence of that and only distracts from the real problem.
     *
     * <p>{@code commandAnalyzerName} is the effective {@code WITH} analyzer - user-written or synthesized by
     * {@code ResolveHighlight} - or {@code null} when absent or not a string.
     */
    private boolean verifyAnalyzerNames(
        Map<String, String> fieldAnalyzerNames,
        String commandAnalyzerName,
        Failures failures,
        AnalysisRegistry analysisRegistry
    ) {
        String commandFailure = unresolvableMessage(commandAnalyzerName, analysisRegistry);
        if (commandFailure != null) {
            // A name the user typed keeps the raw failure even when the same name also labels a borrowed leaf; a name
            // synthesized from a borrowed WHERE is really that leaf's analyzer, so it gets the derived-from-WHERE
            // framing. The synthesis also runs for an explicit query, where there is no WHERE to blame.
            boolean borrowed = implicitQuery && analyzerOrigin == AnalyzerOrigin.WHERE;
            failures.add(fail(this, "{}", borrowed ? borrowedUnresolvedAnalyzerMessage(commandAnalyzerName) : commandFailure));
            return true;
        }
        Set<String> names = new LinkedHashSet<>(fieldAnalyzerNames.values());
        // Quote analyzers reach the query builders by name whatever else is set, so they must always resolve.
        names.addAll(HighlightSupport.quoteAnalyzerNamesOf(query));
        if (implicitQuery == false || commandAnalyzerName == null) {
            // A command analyzer replaces the leaf analyzer options on both the query and the document side (see
            // HighlightQueryBuilders#withoutLeafAnalyzer), so an unresolvable name on a borrowed leaf is harmless -
            // that is what lets WITH rescue a WHERE that used a per-index custom analyzer. A name the user wrote in
            // the HIGHLIGHT query itself is still reported, overridden or not.
            names.addAll(HighlightSupport.leafAnalyzerNamesOf(query));
        }
        names.remove(commandAnalyzerName);
        for (String name : names) {
            String failure = unresolvableMessage(name, analysisRegistry);
            if (failure != null) {
                failures.add(fail(this, "{}", implicitQuery ? borrowedUnresolvedAnalyzerMessage(name) : failure));
                return true;
            }
        }
        return false;
    }

    /** The failure message from resolving {@code name}, or {@code null} when it resolves or is absent. */
    private static String unresolvableMessage(String name, AnalysisRegistry analysisRegistry) {
        if (name == null) {
            return null;
        }
        try {
            PlannerUtils.resolveAnalyzer(name, analysisRegistry);
            return null;
        } catch (InvalidArgumentException e) {
            return e.getMessage();
        }
    }

    private void verifyFieldTypes(Failures failures) {
        for (NamedExpression field : fields) {
            if (field.resolved() && DataType.isString(field.dataType()) == false) {
                failures.add(
                    fail(
                        field,
                        "HIGHLIGHT ON field [{}] must be [text] or [keyword], found [{}]",
                        field.name(),
                        field.dataType().typeName()
                    )
                );
            }
        }
    }

    // WITH { ... } yields constants and parameters, which all fold; the parser rejects map values. Anything else is
    // skipped here and fails later at fold time.
    private Expression foldableOption(String name) {
        Expression value = options.get(name);
        return value != null && value.foldable() ? value : null;
    }

    private void verifyEnum(Failures failures, HighlightOptions.EnumOption option) {
        Expression value = foldableOption(option.name());
        if (value == null) {
            return;
        }
        String actual = BytesRefs.toString(value.fold(FoldContext.small()));
        if (option.isValid(actual) == false) {
            failures.add(
                fail(this, "Invalid value [{}] for option [{}] in HIGHLIGHT, expected one of {}", actual, option.name(), option.allowed())
            );
        }
    }

    private void verifyValue(Failures failures, String name) {
        Expression value = foldableOption(name);
        if (value == null) {
            return;
        }
        try {
            HighlightOptions.validate(name, value, FoldContext.small());
        } catch (RuntimeException e) {
            failures.add(fail(this, "Invalid value for option [{}] in HIGHLIGHT: {}", name, e.getMessage()));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) {
            return false;
        }
        Highlight other = (Highlight) o;
        // analyzerOrigin is excluded. It is analysis-only, for error messages, not identity.
        return Objects.equals(prefix, other.prefix)
            && Objects.equals(query, other.query)
            && implicitQuery == other.implicitQuery
            && derivedFields == other.derivedFields
            && Objects.equals(fields, other.fields)
            && Objects.equals(options, other.options)
            && Objects.equals(generatedFields, other.generatedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix, query, implicitQuery, derivedFields, fields, options, generatedFields);
    }
}
