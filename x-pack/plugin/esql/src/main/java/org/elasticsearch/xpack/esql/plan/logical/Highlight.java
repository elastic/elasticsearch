/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

// TODO: add option-value verification (e.g. encoder must be default|html, order must be none|score).
// TODO: carry an analyzer name here once the "analyzer" option is supported.
public class Highlight extends UnaryPlan implements TelemetryAware, GeneratingPlan<Highlight> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Highlight",
        Highlight::new
    );

    public static final String DEFAULT_PREFIX = "highlight_";

    // Options honoured today by HighlightOptions and the operator.
    public static final String PRE_TAGS = "pre_tags";
    public static final String POST_TAGS = "post_tags";
    public static final String NUMBER_OF_FRAGMENTS = "number_of_fragments";
    public static final String FRAGMENT_SIZE = "fragment_size";
    public static final String ENCODER = "encoder";
    public static final String NO_MATCH_SIZE = "no_match_size";

    // Options the parser accepts but that are not wired through yet. The feature is snapshot-only, so accepting them now
    // keeps the grammar stable; each is honoured in a follow-up. TODO: wire these through HighlightOptions and the operator.
    public static final String BOUNDARY_SCANNER = "boundary_scanner";
    public static final String BOUNDARY_SCANNER_LOCALE = "boundary_scanner_locale";
    public static final String BOUNDARY_CHARS = "boundary_chars";
    public static final String BOUNDARY_MAX_SCAN = "boundary_max_scan";
    public static final String ORDER = "order";
    public static final String MAX_ANALYZED_OFFSET = "max_analyzed_offset";
    public static final String PHRASE_LIMIT = "phrase_limit";

    private static final List<String> VALID_OPTION_NAMES = List.of(
        PRE_TAGS,
        POST_TAGS,
        NUMBER_OF_FRAGMENTS,
        FRAGMENT_SIZE,
        ENCODER,
        BOUNDARY_SCANNER,
        BOUNDARY_SCANNER_LOCALE,
        BOUNDARY_CHARS,
        BOUNDARY_MAX_SCAN,
        ORDER,
        NO_MATCH_SIZE,
        MAX_ANALYZED_OFFSET,
        PHRASE_LIMIT
    );

    private final String prefix;
    private final Expression query;
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
        List<NamedExpression> fields,
        MapExpression options,
        List<Attribute> generatedFields
    ) {
        super(source, child);
        this.prefix = prefix;
        this.query = query;
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
     * Builds the {@code <prefix><field>} keyword output attributes for the given highlight fields.
     * The attributes are nullable because a field that the query does not match yields {@code null}.
     */
    public static List<Attribute> generatedAttributesFor(Source source, String prefix, List<NamedExpression> fields) {
        return fields.stream()
            .map(f -> (Attribute) new ReferenceAttribute(source, null, prefix + f.name(), DataType.KEYWORD, Nullability.TRUE, null, false))
            .toList();
    }

    public Highlight withOptions(MapExpression newOptions) {
        if (Objects.equals(options, newOptions)) {
            return this;
        }
        return new Highlight(source(), child(), prefix, query, fields, newOptions, generatedFields);
    }

    @Override
    public Highlight replaceChild(LogicalPlan newChild) {
        return new Highlight(source(), newChild, prefix, query, fields, options, generatedFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Highlight::new, child(), prefix, query, fields, options, generatedFields);
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
        return new Highlight(source(), child(), prefix, query, fields, options, renamed);
    }

    @Override
    protected AttributeSet computeReferences() {
        // Only the ON fields are inputs; the generated highlight_<field> columns are outputs, not references.
        return Expressions.references(fields);
    }

    @Override
    public boolean expressionsResolved() {
        if (query != null && query.resolved() == false) {
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
    public boolean equals(Object o) {
        if (super.equals(o) == false) {
            return false;
        }
        Highlight other = (Highlight) o;
        return Objects.equals(prefix, other.prefix)
            && Objects.equals(query, other.query)
            && Objects.equals(fields, other.fields)
            && Objects.equals(options, other.options)
            && Objects.equals(generatedFields, other.generatedFields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix, query, fields, options, generatedFields);
    }
}
