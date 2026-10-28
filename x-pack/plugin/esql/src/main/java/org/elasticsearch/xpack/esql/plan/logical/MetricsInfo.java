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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * A {@code MetricsInfo} is a plan node that returns one row per metric with metadata.
 * It must be used before the first STATS command and works with TS source commands.
 */
public class MetricsInfo extends UnaryPlan implements TelemetryAware, PostAnalysisVerificationAware, PipelineBreaker {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "MetricsInfo",
        MetricsInfo::new
    );

    public static final List<String> ATTRIBUTES = List.of(
        "metric_name",
        "data_stream",
        "unit",
        "metric_type",
        "field_type",
        "dimension_fields"
    );

    public static final TransportVersion ESQL_METRICS_INFO_LIMIT = TransportVersion.fromName("esql_metrics_info_limit");

    private final List<Attribute> attributes;
    @Nullable
    private final Integer limit;

    public MetricsInfo(Source source, LogicalPlan child) {
        this(source, child, null);
    }

    /** Carries a LIMIT hint for sampling while preserving the user's output limit above this node. */
    public MetricsInfo(Source source, LogicalPlan child, @Nullable Integer limit) {
        this(source, child, buildAttributes(source), limit);
    }

    private MetricsInfo(Source source, LogicalPlan child, List<Attribute> attributes, @Nullable Integer limit) {
        super(source, child);
        this.attributes = attributes;
        this.limit = limit;
    }

    @Override
    public List<Attribute> output() {
        return attributes;
    }

    private MetricsInfo(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.getTransportVersion().supports(ESQL_METRICS_INFO_LIMIT) ? in.readOptionalVInt() : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        if (out.getTransportVersion().supports(ESQL_METRICS_INFO_LIMIT)) {
            out.writeOptionalVInt(limit);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<MetricsInfo> info() {
        return NodeInfo.create(this, MetricsInfo::new, child(), limit);
    }

    @Override
    public MetricsInfo replaceChild(LogicalPlan newChild) {
        return new MetricsInfo(source(), newChild, attributes, limit);
    }

    @Nullable
    public Integer limit() {
        return limit;
    }

    /** Preserves output attributes when changing the sampling hint during optimization. */
    public MetricsInfo withLimit(@Nullable Integer newLimit) {
        return new MetricsInfo(source(), child(), attributes, newLimit);
    }

    @Override
    public String telemetryLabel() {
        return "METRICS_INFO";
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    protected AttributeSet computeReferences() {
        for (Attribute attribute : child().outputSet()) {
            if (EsQueryExec.isDocAttribute(attribute)) {
                return AttributeSet.of(attribute);
            }
        }
        return child().outputSet();
    }

    @Override
    public int hashCode() {
        return Objects.hash(child(), limit);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        MetricsInfo other = (MetricsInfo) obj;
        return Objects.equals(child(), other.child()) && Objects.equals(limit, other.limit);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        child().forEachDown(p -> {
            if (p instanceof PipelineBreaker) {
                failures.add(fail(this, "METRICS_INFO cannot be used after {} command", pipelineBreakerCommandName(p)));
            }
        });
        if (failures.hasFailures()) {
            return;
        }
        boolean hasTsSource = child().anyMatch(p -> p instanceof EsRelation er && er.indexMode().isTsdb());
        if (hasTsSource == false) {
            failures.add(fail(this, "METRICS_INFO can only be used with TS source command"));
        }
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
