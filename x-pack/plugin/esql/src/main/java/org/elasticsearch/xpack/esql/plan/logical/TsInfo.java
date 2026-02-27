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
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * A {@code TsInfo} is a plan node that returns one row per time series with metadata.
 * It includes all columns from {@link MetricsInfo} plus a {@code dimensions} column
 * containing a JSON-encoded representation of dimension key/values.
 * <p>
 * Must be used before the first STATS command and works only with TS source commands.
 */
public class TsInfo extends UnaryPlan implements TelemetryAware, PostAnalysisVerificationAware, PipelineBreaker {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "TsInfo", TsInfo::new);

    public static final List<String> ATTRIBUTES = List.of(
        "metric_name",
        "data_stream",
        "unit",
        "metric_type",
        "field_type",
        "dimension_fields",
        "dimensions"
    );

    private final List<Attribute> attributes;

    public TsInfo(Source source, LogicalPlan child) {
        this(source, child, buildAttributes(source));
    }

    private TsInfo(Source source, LogicalPlan child, List<Attribute> attributes) {
        super(source, child);
        this.attributes = attributes;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    private TsInfo(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteableCollection(attributes);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<TsInfo> info() {
        return NodeInfo.create(this, TsInfo::new, child());
    }

    @Override
    public TsInfo replaceChild(LogicalPlan newChild) {
        return new TsInfo(source(), newChild, attributes);
    }

    @Override
    public String telemetryLabel() {
        return "TS_INFO";
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected AttributeSet computeReferences() {
        return AttributeSet.EMPTY;
    }

    @Override
    public int hashCode() {
        return Objects.hash(child());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TsInfo other = (TsInfo) obj;
        return Objects.equals(child(), other.child());
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        boolean hasTsSource = child().anyMatch(p -> p instanceof EsRelation er && er.indexMode() == IndexMode.TIME_SERIES);
        if (hasTsSource == false) {
            failures.add(fail(this, "TS_INFO can only be used with TS source command"));
        }

        child().forEachDown(p -> {
            if (p instanceof PipelineBreaker) {
                failures.add(fail(this, "TS_INFO cannot be used after {} command", pipelineBreakerCommandName(p)));
            }
        });
    }

    private static String pipelineBreakerCommandName(LogicalPlan plan) {
        if (plan instanceof Aggregate) {
            return "STATS";
        } else if (plan instanceof Limit) {
            return "LIMIT";
        } else if (plan instanceof OrderBy) {
            return "SORT";
        } else {
            return plan.nodeName();
        }
    }

    private static List<Attribute> buildAttributes(Source source) {
        List<Attribute> attributes = new ArrayList<>();
        for (var name : ATTRIBUTES) {
            attributes.add(new ReferenceAttribute(source, null, name, KEYWORD));
        }
        return attributes;
    }
}
