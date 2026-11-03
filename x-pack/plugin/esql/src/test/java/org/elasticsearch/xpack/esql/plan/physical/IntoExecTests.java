/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.into.IntoOptions;
import org.elasticsearch.xpack.esql.into.IntoStats;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class IntoExecTests extends ESTestCase {

    private static PhysicalPlan child() {
        return new LocalSourceExec(
            Source.EMPTY,
            List.of(new ReferenceAttribute(Source.EMPTY, "host", DataType.KEYWORD)),
            LocalSupplier.of(new Page(0))
        );
    }

    /** The plan answers with the statistics row, whatever its child produces. */
    public void testOutputIsTheStatsSchema() {
        IntoExec into = new IntoExec(Source.EMPTY, child(), "dst", IntoOptions.DEFAULT, null);
        assertThat(into.output(), equalTo(IntoStats.ATTRIBUTES));
        assertThat(into.output(), not(equalTo(into.child().output())));
    }

    /** The writer is attached by the compute service and travels with the node, but does not take part in equality. */
    public void testWithWriter() {
        IntoExec into = new IntoExec(Source.EMPTY, child(), "dst", IntoOptions.DEFAULT, null);
        assertThat(into.writer(), nullValue());
        PageConsumer writer = new PageConsumer() {
            @Override
            public void accept(Page page) {}

            @Override
            public IsBlockedResult isBlocked() {
                return Operator.NOT_BLOCKED;
            }

            @Override
            public void finish() {}

            @Override
            public boolean isFinished() {
                return true;
            }

            @Override
            public void close() {}
        };
        IntoExec withWriter = into.withWriter(writer);
        assertThat(withWriter.writer(), sameInstance(writer));
        assertThat(withWriter.destination(), equalTo("dst"));
        assertThat(withWriter.options(), equalTo(IntoOptions.DEFAULT));
        assertThat(withWriter, equalTo(into));
        assertThat(((IntoExec) withWriter.replaceChild(child())).writer(), sameInstance(writer));
    }

    public void testEqualityCoversDestinationAndOptions() {
        IntoExec into = new IntoExec(Source.EMPTY, child(), "dst", IntoOptions.DEFAULT, null);
        assertThat(into, not(equalTo(new IntoExec(Source.EMPTY, child(), "dst", IntoOptions.DEFAULT.with("collisions", "keep"), null))));
        assertThat(into, not(equalTo(new IntoExec(Source.EMPTY, child(), "other", IntoOptions.DEFAULT, null))));
        assertThat(into, not(equalTo(new IntoExec(Source.EMPTY, child(), "dst", IntoOptions.DEFAULT.with("refresh", false), null))));
    }

    public void testIsNeverSerialized() {
        IntoExec into = new IntoExec(Source.EMPTY, child(), "dst", IntoOptions.DEFAULT, null);
        expectThrows(UnsupportedOperationException.class, into::getWriteableName);
    }
}
