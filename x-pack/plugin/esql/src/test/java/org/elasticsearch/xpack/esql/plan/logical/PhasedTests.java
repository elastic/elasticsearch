/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.index.EsIndex;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PhasedTests extends ESTestCase {
    public void testZeroLayers() {
        EsRelation relation = new EsRelation(Source.synthetic("relation"), new EsIndex("foo", Map.of()), IndexMode.STANDARD, false);
        relation.setOptimized();
        assertThat(Phased.extractFirstPhase(relation), nullValue());
    }

    public void testOneLayer() {
        EsRelation relation = new EsRelation(Source.synthetic("relation"), new EsIndex("foo", Map.of()), IndexMode.STANDARD, false);
        LogicalPlan orig = new Dummy(Source.synthetic("orig"), relation);
        orig.setOptimized();
        assertThat(Phased.extractFirstPhase(orig), sameInstance(relation));
        LogicalPlan finalPhase = Phased.applyResultsFromFirstPhase(
            orig,
            List.of(new ReferenceAttribute(Source.EMPTY, "foo", DataType.KEYWORD)),
            List.of()
        );
        assertThat(
            finalPhase,
            equalTo(new Row(orig.source(), List.of(new Alias(orig.source(), "foo", new Literal(orig.source(), "foo", DataType.KEYWORD)))))
        );
        finalPhase.setOptimized();
        assertThat(Phased.extractFirstPhase(finalPhase), nullValue());
    }

    public void testTwoLayer() {
        EsRelation relation = new EsRelation(Source.synthetic("relation"), new EsIndex("foo", Map.of()), IndexMode.STANDARD, false);
        LogicalPlan inner = new Dummy(Source.synthetic("inner"), relation);
        LogicalPlan orig = new Dummy(Source.synthetic("outer"), inner);
        orig.setOptimized();
        assertThat(
            "extractFirstPhase should call #firstPhase on the earliest child in the plan",
            Phased.extractFirstPhase(orig),
            sameInstance(relation)
        );
        LogicalPlan secondPhase = Phased.applyResultsFromFirstPhase(
            orig,
            List.of(new ReferenceAttribute(Source.EMPTY, "foo", DataType.KEYWORD)),
            List.of()
        );
        secondPhase.setOptimized();
        assertThat(
            "applyResultsFromFirstPhase should call #nextPhase one th earliest child in the plan",
            secondPhase,
            equalTo(
                new Dummy(
                    Source.synthetic("outer"),
                    new Row(orig.source(), List.of(new Alias(orig.source(), "foo", new Literal(orig.source(), "foo", DataType.KEYWORD))))
                )
            )
        );

        assertThat(Phased.extractFirstPhase(secondPhase), sameInstance(secondPhase.children().get(0)));
        LogicalPlan finalPhase = Phased.applyResultsFromFirstPhase(
            secondPhase,
            List.of(new ReferenceAttribute(Source.EMPTY, "foo", DataType.KEYWORD)),
            List.of()
        );
        finalPhase.setOptimized();
        assertThat(
            finalPhase,
            equalTo(new Row(orig.source(), List.of(new Alias(orig.source(), "foo", new Literal(orig.source(), "foo", DataType.KEYWORD)))))
        );

        assertThat(Phased.extractFirstPhase(finalPhase), nullValue());
    }

    public class Dummy extends UnaryPlan implements Phased {
        Dummy(Source source, LogicalPlan child) {
            super(source, child);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException("not serialized");
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException("not serialized");
        }

        @Override
        public boolean expressionsResolved() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected NodeInfo<? extends LogicalPlan> info() {
            return NodeInfo.create(this, Dummy::new, child());
        }

        @Override
        public int hashCode() {
            return child().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Dummy == false) {
                return false;
            }
            Dummy other = (Dummy) obj;
            return child().equals(other.child());
        }

        @Override
        public UnaryPlan replaceChild(LogicalPlan newChild) {
            return new Dummy(source(), newChild);
        }

        @Override
        public List<Attribute> output() {
            return child().output();
        }

        @Override
        public LogicalPlan firstPhase() {
            return child();
        }

        @Override
        public LogicalPlan nextPhase(List<Attribute> schema, List<Page> firstPhaseResult) {
            // Replace myself with a dummy "row" command
            return new Row(
                source(),
                schema.stream().map(a -> new Alias(source(), a.name(), new Literal(source(), a.name(), DataType.KEYWORD))).toList()
            );
        }
    }
}
