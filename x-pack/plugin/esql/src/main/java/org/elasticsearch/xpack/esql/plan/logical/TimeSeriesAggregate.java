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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * An extension of {@link Aggregate} to perform time-series aggregation per time-series, such as rate or _over_time.
 * The grouping must be `_tsid` and `tbucket` or just `_tsid`.
 */
public class TimeSeriesAggregate extends Aggregate {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TimeSeriesAggregate",
        TimeSeriesAggregate::new
    );

    private final Bucket timeBucket;

    public TimeSeriesAggregate(
        Source source,
        LogicalPlan child,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates,
        Bucket timeBucket
    ) {
        super(source, child, groupings, aggregates);
        this.timeBucket = timeBucket;
    }

    public TimeSeriesAggregate(StreamInput in) throws IOException {
        super(in);
        this.timeBucket = in.readOptionalWriteable(inp -> (Bucket) Bucket.ENTRY.reader.read(inp));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(timeBucket);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, TimeSeriesAggregate::new, child(), groupings, aggregates, timeBucket);
    }

    @Override
    public TimeSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new TimeSeriesAggregate(source(), newChild, groupings, aggregates, timeBucket);
    }

    @Override
    public TimeSeriesAggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new TimeSeriesAggregate(source(), child, newGroupings, newAggregates, timeBucket);
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && (timeBucket == null || timeBucket.resolved());
    }

    @Nullable
    public Bucket timeBucket() {
        return timeBucket;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, child(), timeBucket);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TimeSeriesAggregate other = (TimeSeriesAggregate) obj;
        return Objects.equals(groupings, other.groupings)
            && Objects.equals(aggregates, other.aggregates)
            && Objects.equals(child(), other.child())
            && Objects.equals(timeBucket, other.timeBucket);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        super.postAnalysisVerification(failures);
        child().forEachDown(p -> {
            // reject `TS metrics | SORT BY ... | STATS ...`
            if (p instanceof OrderBy orderBy) {
                failures.add(
                    fail(
                        orderBy,
                        "sorting [{}] between the time-series source and the first aggregation [{}] is not allowed",
                        orderBy.sourceText(),
                        this.sourceText()
                    )
                );
            }
            // reject `TS metrics | LIMIT ... | STATS ...`
            if (p instanceof Limit limit) {
                failures.add(
                    fail(
                        limit,
                        "limiting [{}] the time-series source before the first aggregation [{}] is not allowed; "
                            + "filter data with a WHERE command instead",
                        limit.sourceText(),
                        this.sourceText()
                    )
                );
            }
        });
    }

    @Override
    protected void checkTimeSeriesAggregates(Failures failures) {
        List<TimeSeriesAggregateFunction> overTimeFunctions = new ArrayList<>();
        for (NamedExpression aggregate : aggregates) {
            if (aggregate instanceof Alias alias && Alias.unwrap(alias) instanceof AggregateFunction outer) {
                if (outer instanceof TimeSeriesAggregateFunction ts) {
                    outer.field()
                        .forEachDown(
                            AggregateFunction.class,
                            nested -> failures.add(
                                fail(
                                    this,
                                    "cannot use aggregate function [{}] inside over-time aggregation function [{}]",
                                    nested.sourceText(),
                                    outer.sourceText()
                                )
                            )
                        );
                    // reject `TS metrics | STATS rate(requests)`
                    // TODO: support this
                    failures.add(
                        fail(
                            ts,
                            "over-time aggregate function [{}] can only be used with the TS command and inside another aggregate function",
                            ts.sourceText()
                        )
                    );
                    overTimeFunctions.add(ts);
                } else {
                    outer.field().forEachDown(AggregateFunction.class, nested -> {
                        if (nested instanceof TimeSeriesAggregateFunction == false) {
                            fail(
                                this,
                                "cannot use aggregate function [{}] inside aggregation function [{}];"
                                    + "only over-time aggregation function can be used inside another aggregation function",
                                nested.sourceText(),
                                outer.sourceText()
                            );
                        }
                        nested.field()
                            .forEachDown(
                                AggregateFunction.class,
                                nested2 -> failures.add(
                                    fail(
                                        this,
                                        "cannot use aggregate function [{}] inside over-time aggregation function [{}]",
                                        nested.sourceText(),
                                        nested2.sourceText()
                                    )
                                )
                            );
                    });
                }
            }
        }
        // reject `TS metrics | STATS rate(requests) BY host, TBUCKET(1 hour)`
        if (overTimeFunctions.isEmpty() == false) {
            Holder<Attribute> timestamp = new Holder<>();
            forEachDown(EsRelation.class, r -> {
                for (Attribute attr : r.output()) {
                    if (attr.name().equals(MetadataAttribute.TIMESTAMP_FIELD)) {
                        timestamp.set(attr);
                    }
                }
            });
            List<Expression> nonTimeBucketGroupings = new ArrayList<>();
            for (Expression g : groupings) {
                boolean timeBucket = g.anyMatch(
                    c -> (c instanceof Bucket b && b.field().equals(timestamp.get())
                        || (c instanceof TBucket tb && tb.field().equals(timestamp.get())))
                );
                if (timeBucket == false) {
                    nonTimeBucketGroupings.add(g);
                }
            }
            if (nonTimeBucketGroupings.isEmpty() == false) {
                for (TimeSeriesAggregateFunction af : overTimeFunctions) {
                    failures.add(
                        fail(
                            this,
                            "cannot use over-time aggregate function [{}] with groupings [{}] other than the time bucket; "
                                + "drop the groupings or provide an outer aggregation",
                            af.sourceText(),
                            nonTimeBucketGroupings.stream().map(Node::sourceText).collect(Collectors.joining(", "))
                        )
                    );
                }
            }
        }
    }
}
