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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class Highlight extends UnaryPlan implements TelemetryAware {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "Highlight",
        Highlight::new
    );

    public static final String DEFAULT_PREFIX = "highlight_";

    public static final String PRE_TAGS = "pre_tags";
    public static final String POST_TAGS = "post_tags";
    public static final String NUMBER_OF_FRAGMENTS = "number_of_fragments";
    public static final String FRAGMENT_SIZE = "fragment_size";
    public static final String ENCODER = "encoder";
    public static final String BOUNDARY_SCANNER = "boundary_scanner";
    public static final String BOUNDARY_SCANNER_LOCALE = "boundary_scanner_locale";
    public static final String BOUNDARY_CHARS = "boundary_chars";
    public static final String BOUNDARY_MAX_SCAN = "boundary_max_scan";
    public static final String ORDER = "order";
    public static final String NO_MATCH_SIZE = "no_match_size";
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
    private final List<Expression> fields;
    private final MapExpression options;

    public Highlight(Source source, LogicalPlan child, String prefix, Expression query, List<Expression> fields, MapExpression options) {
        super(source, child);
        this.prefix = prefix;
        this.query = query;
        this.fields = fields;
        this.options = options;
    }

    private Highlight(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readString(),
            in.readOptionalNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(Expression.class),
            in.readOptionalNamedWriteable(MapExpression.class)
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

    public List<Expression> fields() {
        return fields;
    }

    public MapExpression options() {
        return options;
    }

    public static List<String> validOptionNames() {
        return VALID_OPTION_NAMES;
    }

    public Highlight withOptions(MapExpression newOptions) {
        if (Objects.equals(options, newOptions)) {
            return this;
        }
        return new Highlight(source(), child(), prefix, query, fields, newOptions);
    }

    @Override
    public Highlight replaceChild(LogicalPlan newChild) {
        return new Highlight(source(), newChild, prefix, query, fields, options);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Highlight::new, child(), prefix, query, fields, options);
    }

    @Override
    public boolean expressionsResolved() {
        if (query != null && query.resolved() == false) {
            return false;
        }
        for (Expression field : fields) {
            if (field.resolved() == false) {
                return false;
            }
        }
        return options == null || options.resolved();
    }

    // TODO: once we add execution, output() must add `<prefix><field>` columns (or replace
    // the original columns when prefix is the empty string)
    // For now keep pass-through so downstream KEEP / EXPLAIN sees the same schema as without HIGHLIGHT.

    @Override
    public boolean equals(Object o) {
        if (super.equals(o) == false) {
            return false;
        }
        Highlight other = (Highlight) o;
        return Objects.equals(prefix, other.prefix)
            && Objects.equals(query, other.query)
            && Objects.equals(fields, other.fields)
            && Objects.equals(options, other.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), prefix, query, fields, options);
    }
}
