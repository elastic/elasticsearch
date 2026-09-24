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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.NamedAnalyzer;
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
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.highlight.HighlightAnalyzers;
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
import java.util.function.Consumer;

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
    /** True when ON was omitted or is {@code *}. Kept after the field list is filled in. */
    private final boolean derivedFields;
    private final List<NamedExpression> fields;
    private final MapExpression options;
    /** Generated {@code <prefix><field>} attributes, appended in ON-field order. */
    private final List<Attribute> generatedFields;
    /**
     * The {@code _index} column of each row, set by the analyzer when the queried indices disagree on an ON field's
     * analyzer so each row can be highlighted with its own index's analyzer. {@code null} otherwise.
     */
    private final @Nullable Attribute indexKey;
    /**
     * The mapping of each ON column that FORK or UNION ALL merged from mapped text fields, by name: the merged column is a
     * {@link ReferenceAttribute}, which does not carry it. Set by the analyzer, empty otherwise.
     */
    private final Map<String, TextEsField> fieldMappings;

    public Highlight(
        Source source,
        LogicalPlan child,
        String prefix,
        Expression query,
        boolean implicitQuery,
        boolean derivedFields,
        List<NamedExpression> fields,
        MapExpression options,
        List<Attribute> generatedFields,
        @Nullable Attribute indexKey,
        Map<String, TextEsField> fieldMappings
    ) {
        super(source, child);
        this.prefix = prefix;
        this.query = query;
        this.implicitQuery = implicitQuery;
        this.derivedFields = derivedFields;
        this.fields = fields;
        this.options = options;
        this.generatedFields = generatedFields;
        this.indexKey = indexKey;
        this.fieldMappings = fieldMappings;
    }

    private Highlight(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readString(),
            in.readOptionalNamedWriteable(Expression.class),
            in.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS) ? in.readBoolean() : false,
            in.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS) ? in.readBoolean() : false,
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            // MapExpression is registered under the Expression category, not its own, so read it as an Expression.
            (MapExpression) in.readOptionalNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS)
                ? in.readOptionalNamedWriteable(Attribute.class)
                : null,
            in.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS) ? in.readImmutableMap(EsField::readFrom) : Map.of()
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
        if (out.getTransportVersion().supports(ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS)) {
            // The analyzer only sets the key and the mappings when every node supports this version.
            out.writeOptionalNamedWriteable(indexKey);
            out.writeMap(fieldMappings, (o, mapping) -> mapping.writeTo(o));
        }
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

    public List<NamedExpression> fields() {
        return fields;
    }

    public MapExpression options() {
        return options;
    }

    public Attribute indexKey() {
        return indexKey;
    }

    public Map<String, TextEsField> fieldMappings() {
        return fieldMappings;
    }

    /** Whether WITH sets {@code analyzer}, which then applies to every row instead of each field's mapping analyzer. */
    public boolean hasAnalyzerOption() {
        return options != null && options.get(ANALYZER) != null;
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
            fields,
            options,
            generatedFields,
            indexKey,
            fieldMappings
        );
    }

    public Highlight withOptions(MapExpression newOptions) {
        if (Objects.equals(options, newOptions)) {
            return this;
        }
        return copy(child(), query, fields, newOptions, generatedFields);
    }

    /** A non-null {@code key} must be in {@code newChild}'s output. */
    public Highlight withIndexKeyAndMappings(LogicalPlan newChild, @Nullable Attribute key, Map<String, TextEsField> newFieldMappings) {
        assert key == null || newChild.outputSet().contains(key) : "HIGHLIGHT index key must be in the child output";
        return new Highlight(
            source(),
            newChild,
            prefix,
            query,
            implicitQuery,
            derivedFields,
            fields,
            options,
            generatedFields,
            key,
            newFieldMappings
        );
    }

    /**
     * Keeps {@link #derivedFields}. Pass {@code newGeneratedFields} unchanged unless {@code newFields} changed:
     * {@code generatedAttributesFor} mints fresh {@link NameId}s on every call.
     */
    public Highlight withResolved(
        Expression newQuery,
        boolean newImplicitQuery,
        List<NamedExpression> newFields,
        List<Attribute> newGeneratedFields
    ) {
        return new Highlight(
            source(),
            child(),
            prefix,
            newQuery,
            newImplicitQuery,
            derivedFields,
            newFields,
            options,
            newGeneratedFields,
            indexKey,
            fieldMappings
        );
    }

    /**
     * Subset of ON fields and aligned generated columns. After verification; do not prune a field the query still translates against.
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
            fields,
            options,
            generatedFields,
            indexKey,
            fieldMappings
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
        // The ON fields and the index key are inputs; the generated <prefix><field> columns are outputs, not references.
        return Expressions.references(indexKey == null ? fields : CollectionUtils.combine(fields, indexKey));
    }

    @Override
    public boolean expressionsResolved() {
        // Empty ON: still deriving fields. ON * is UnresolvedStar and fails the loop below, not isEmpty().
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
    public void postAnalysisVerification(AnalysisRegistry analysisRegistry, Consumer<String> warnings, Failures failures) {
        postAnalysisVerification(failures);
        if (query == null || query.resolved() == false || fields.isEmpty()) {
            return;
        }
        String commandAnalyzerName;
        try {
            commandAnalyzerName = analyzerOptionName();
        } catch (IllegalArgumentException e) {
            // The analyzer value isn't a string. Type errors have already been reported by verifyValue.
            commandAnalyzerName = null;
        }
        if (verifyAnalyzerNames(commandAnalyzerName, failures, analysisRegistry)) {
            return;
        }
        verifyQuery(commandAnalyzerName, failures, analysisRegistry, warnings);
    }

    private String analyzerOptionName() {
        Expression value = options == null ? null : foldableOption(ANALYZER);
        return value == null ? null : HighlightOptions.analyzerName(ANALYZER, value, FoldContext.small());
    }

    /** WITH cannot clear this: the leaf option is query-side. */
    private static String borrowedUnresolvedAnalyzerMessage(String name) {
        return "HIGHLIGHT derived its query from a preceding WHERE, but that query refers to analyzer ["
            + name
            + "], which is not a registered analyzer. Write the query on HIGHLIGHT without that analyzer option, "
            + "or drop the option from the WHERE; highlights may then differ from what matched.";
    }

    private void verifyQuery(String commandAnalyzerName, Failures failures, AnalysisRegistry analysisRegistry, Consumer<String> warnings) {
        try {
            // TO_TEXT declarations may not have been verified yet.
            HighlightAnalyzers.Resolved resolved = HighlightAnalyzers.resolve(
                fields,
                fieldMappings,
                commandAnalyzerName,
                analysisRegistry,
                indexKey != null,
                warnings
            );
            // Enforce ON membership only when the query and field list are both explicit. An implicit query
            // treats a field outside ON as match-none instead of failing.
            for (Map<String, NamedAnalyzer> fieldAnalyzers : resolved.analysisGroups()) {
                HighlightQueryBuilders.verify(
                    query,
                    fieldAnalyzers,
                    implicitQuery == false && derivedFields == false,
                    implicitQuery,
                    analysisRegistry
                );
            }
        } catch (InvalidArgumentException | IllegalArgumentException e) {
            // Attach to the query node, not this Highlight node: failures dedupe by node, so pinning it here would let a
            // co-located option/analyzer failure on this node swallow the query error (see VerifierTests#testHighlightAnalyzerOption).
            failures.add(fail(query, "{}", e.getMessage()));
        }
    }

    /** Returns {@code true} when a WITH or query analyzer name failed to resolve. Mapping names are not checked here. */
    private boolean verifyAnalyzerNames(String commandAnalyzerName, Failures failures, AnalysisRegistry analysisRegistry) {
        Set<String> names = new LinkedHashSet<>();
        if (commandAnalyzerName != null) {
            names.add(commandAnalyzerName);
        }
        names.addAll(HighlightSupport.analyzerNamesOf(query));
        for (String name : names) {
            try {
                PlannerUtils.resolveAnalyzer(name, analysisRegistry);
            } catch (InvalidArgumentException e) {
                boolean borrowed = implicitQuery && name.equals(commandAnalyzerName) == false;
                failures.add(fail(this, "{}", borrowed ? borrowedUnresolvedAnalyzerMessage(name) : e.getMessage()));
                return true;
            }
        }
        return false;
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
        return Objects.equals(prefix, other.prefix)
            && Objects.equals(query, other.query)
            && implicitQuery == other.implicitQuery
            && derivedFields == other.derivedFields
            && Objects.equals(fields, other.fields)
            && Objects.equals(options, other.options)
            && Objects.equals(generatedFields, other.generatedFields)
            && Objects.equals(indexKey, other.indexKey)
            && Objects.equals(fieldMappings, other.fieldMappings);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            prefix,
            query,
            implicitQuery,
            derivedFields,
            fields,
            options,
            generatedFields,
            indexKey,
            fieldMappings
        );
    }
}
