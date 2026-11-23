/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.timeseries;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlLabels;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Internal function that mutates a keyword {@code _timeseries} metadata container; the value replaces the dimension values, and a
 * missing or empty value removes the dimension. Several values write a multi-valued dimension. The label is written at {@code path},
 * the keys of the source field that holds it, such as {@code [labels, pod]}, so a label written back into a container where the
 * source held it reads as the source does; a field holding the label elsewhere is removed.
 */
public final class TimeSeriesMetadataSet extends AbstractTimeSeriesMetadataFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "TimeSeriesMetadataSet",
        TimeSeriesMetadataSet::new
    );

    private final List<String> path;

    public TimeSeriesMetadataSet(Source source, Expression labelContainer, Expression label, Expression value, List<String> path) {
        super(source, List.of(labelContainer, label, value));
        this.path = path;
    }

    private TimeSeriesMetadataSet(StreamInput in) throws IOException {
        super(in);
        this.path = in.readStringCollectionAsImmutableList();
    }

    @Override
    protected void writeArguments(StreamOutput out) throws IOException {
        out.writeStringCollection(path);
    }

    /** The keys of the source field the label is written to. */
    public List<String> path() {
        return path;
    }

    public Expression label() {
        return children().get(1);
    }

    public Expression value() {
        return children().get(2);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        return new TimeSeriesMetadataSetEvaluator.Factory(
            toEvaluator.apply(children().getFirst()),
            foldLabelName(label(), toEvaluator),
            path,
            toEvaluator.apply(value())
        );
    }

    @Override
    public List<Expression> labels() {
        return List.of(label());
    }

    @Override
    protected TypeResolution resolveType() {
        TypeResolution resolution = super.resolveType().and(
            TypeResolutions.isString(value(), sourceText(), TypeResolutions.ParamOrdinal.THIRD)
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        String label = ((BytesRef) label().fold(FoldContext.small())).utf8ToString();
        return path.isEmpty() == false && PromqlLabels.labelName(String.join(".", path)).equals(label)
            ? resolution
            : new TypeResolution("[" + sourceText() + "] writes label [" + label + "] to path " + path + " of another label");
    }

    @Override
    public TimeSeriesMetadataSet replaceChildren(List<Expression> children) {
        return new TimeSeriesMetadataSet(source(), children.get(0), children.get(1), children.get(2), path);
    }

    @Override
    protected NodeInfo<TimeSeriesMetadataSet> info() {
        return NodeInfo.create(this, TimeSeriesMetadataSet::new, children().getFirst(), label(), value(), path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), path);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && path.equals(((TimeSeriesMetadataSet) obj).path);
    }
}
