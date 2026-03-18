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
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.TypedAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedTimestamp;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.TimestampAware;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;

/**
 * An extension of {@link Aggregate} to perform time-series aggregation per time-series, such as rate or _over_time.
 * The grouping must be `_tsid` and `tbucket` or just `_tsid`.
 */
public class TimeSeriesAggregate extends Aggregate implements TimestampAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "TimeSeriesAggregate",
        TimeSeriesAggregate::new
    );
    private static final TransportVersion TIME_SERIES_AGGREGATE_TIMESTAMP = TransportVersion.fromName("time_series_aggregate_timestamp");

    private final Bucket timeBucket;
    private final Expression timestamp;

    public TimeSeriesAggregate(
        Source source,
        LogicalPlan child,
        List<Expression> groupings,
        List<? extends NamedExpression> aggregates,
        Bucket timeBucket,
        Expression timestamp
    ) {
        super(source, child, groupings, aggregates);
        this.timeBucket = timeBucket;
        this.timestamp = timestamp;
    }

    public TimeSeriesAggregate(StreamInput in) throws IOException {
        super(in);
        this.timeBucket = in.readOptionalWriteable(inp -> (Bucket) Bucket.ENTRY.reader.read(inp));
        if (in.getTransportVersion().supports(TIME_SERIES_AGGREGATE_TIMESTAMP)) {
            this.timestamp = in.readOptionalNamedWriteable(Expression.class);
        } else {
            // We only need the timestamp during analysis and logical optimization on the coordinator.
            // Using null (when deserialized from an old node) in this case should be okay.
            this.timestamp = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(timeBucket);
        if (out.getTransportVersion().supports(TIME_SERIES_AGGREGATE_TIMESTAMP)) {
            out.writeOptionalNamedWriteable(timestamp);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<Aggregate> info() {
        return NodeInfo.create(this, TimeSeriesAggregate::new, child(), groupings, aggregates, timeBucket, timestamp);
    }

    @Override
    public TimeSeriesAggregate replaceChild(LogicalPlan newChild) {
        return new TimeSeriesAggregate(source(), newChild, groupings, aggregates, timeBucket, timestamp);
    }

    @Override
    public TimeSeriesAggregate with(LogicalPlan child, List<Expression> newGroupings, List<? extends NamedExpression> newAggregates) {
        return new TimeSeriesAggregate(source(), child, newGroupings, newAggregates, timeBucket, timestamp);
    }

    public LogicalPlan withTimestamp(Expression newTimestamp) {
        if (newTimestamp.equals(timestamp)) {
            return this;
        }
        return new TimeSeriesAggregate(source(), child(), groupings, aggregates, timeBucket, newTimestamp);
    }

    @Override
    public boolean expressionsResolved() {
        return super.expressionsResolved() && (timeBucket == null || timeBucket.resolved()) && (timestamp == null || timestamp.resolved());
    }

    @Nullable
    public Bucket timeBucket() {
        return timeBucket;
    }

    @Override
    public Expression timestamp() {
        return timestamp;
    }

    @Override
    public int hashCode() {
        return Objects.hash(groupings, aggregates, child(), timeBucket, timestamp);
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
            && Objects.equals(timeBucket, other.timeBucket)
            && Objects.equals(timestamp, other.timestamp);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        super.postAnalysisVerification(failures);
        // We forbid grouping by a metric field itself. Metric fields are allowed only inside aggregate functions.
        groupings().forEach(g -> g.forEachDown(e -> {
            if (e instanceof FieldAttribute fieldAttr && fieldAttr.isMetric()) {
                failures.add(
                    fail(
                        fieldAttr,
                        "cannot group by a metric field [{}] in a time-series aggregation. "
                            + "If you want to group by a metric field, use the FROM "
                            + "command instead of the TS command.",
                        fieldAttr.sourceText()
                    )
                );
            }
        }));
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
            // reject `TS metrics | LOOKUP JOIN ... | STATS ...`
            if (p instanceof LookupJoin lookupJoin) {
                failures.add(
                    fail(
                        lookupJoin,
                        "lookup join [{}] in the time-series before the first aggregation [{}] is not allowed",
                        lookupJoin.sourceText(),
                        this.sourceText()
                    )
                );
            }
            // reject `TS metrics | ENRICH ... | STATS ...`
            if (p instanceof Enrich enrich) {
                failures.add(
                    fail(
                        enrich,
                        "enrich [{}] in the time-series before the first aggregation [{}] is not allowed",
                        enrich.sourceText(),
                        this.sourceText()
                    )
                );
            }
            // reject `TS metrics | CHANGE POINT ... | STATS ...`
            if (p instanceof ChangePoint changePoint) {
                failures.add(
                    fail(
                        changePoint,
                        "change_point [{}] in the time-series the first aggregation [{}] is not allowed",
                        changePoint.sourceText(),
                        this.sourceText()
                    )
                );
            }
            // reject `TS metrics | MV_EXPAND ... | STATS ...`
            if (p instanceof MvExpand mvExpand) {
                failures.add(
                    fail(
                        mvExpand,
                        "mv_expand [{}] in the time-series before the first aggregation [{}] is not allowed",
                        mvExpand.sourceText(),
                        this.sourceText()
                    )
                );
            }
        });
        if ((timestamp instanceof TypedAttribute) == false || timestamp.dataType().isDate() == false) {
            if (timestamp instanceof UnresolvedTimestamp unresolvedTimestamp) {
                failures.add(fail(unresolvedTimestamp, unresolvedTimestamp.unresolvedMessage()));
            } else {
                failures.add(
                    fail(
                        timestamp,
                        "the TS STATS command requires an @timestamp field of type date or date_nanos but it was of type [{}]",
                        timestamp.dataType().typeName()
                    )
                );
            }
        }
    }

    @Override
    protected void checkTimeSeriesAggregates(Failures failures) {
        for (NamedExpression aggregate : aggregates) {
            if (aggregate instanceof Alias alias && Alias.unwrap(alias) instanceof AggregateFunction outer) {
                if (outer instanceof Count count && count.field().foldable()) {
                    // reject `TS metrics | STATS COUNT(*)`
                    failures.add(
                        fail(count, "count_star [{}] can't be used with TS command; use count on a field instead", outer.sourceText())
                    );
                    // reject COUNT(keyword), but allow COUNT(numeric)
                }
                outer.field().forEachDown(AggregateFunction.class, nested -> {
                    if (nested instanceof TimeSeriesAggregateFunction == false) {
                        fail(
                            this,
                            "cannot use aggregate function [{}] inside aggregation function [{}];"
                                + "only time-series aggregation function can be used inside another aggregation function",
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
                // }
            }
        }
    }
}
