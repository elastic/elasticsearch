/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.expression.function.scalar.string;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.ql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.ql.util.Check;

import java.io.IOException;
import java.util.Objects;

public class BetweenFunctionProcessor implements Processor {

    public static final String NAME = "sbtw";

    private final Processor input, left, right, greedy;
    private final boolean caseInsensitive;

    public BetweenFunctionProcessor(Processor input, Processor left, Processor right, Processor greedy, boolean caseInsensitive) {
        this.input = input;
        this.left = left;
        this.right = right;
        this.greedy = greedy;
        this.caseInsensitive = caseInsensitive;
    }

    public BetweenFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
        left = in.readNamedWriteable(Processor.class);
        right = in.readNamedWriteable(Processor.class);
        greedy = in.readNamedWriteable(Processor.class);
        caseInsensitive = in.readBoolean();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
        out.writeNamedWriteable(greedy);
        out.writeBoolean(caseInsensitive);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object o) {
        return doProcess(input.process(o), left.process(o), right.process(o), greedy.process(o), caseInsensitive);
    }

    public static Object doProcess(Object input, Object left, Object right, Object greedy, Boolean caseInsensitive) {
        if (input == null) {
            return null;
        }

        Check.isString(input);
        Check.isString(left);
        Check.isString(right);

        Check.isBoolean(greedy);
        Check.isBoolean(caseInsensitive);

        String str = input.toString();
        String strRight = right.toString();
        String strLeft = left.toString();
        boolean bGreedy = ((Boolean) greedy).booleanValue();
        return StringUtils.between(str, strLeft, strRight, bGreedy, caseInsensitive);
    }

    protected Processor input() {
        return input;
    }

    public Processor left() {
        return left;
    }

    public Processor right() {
        return right;
    }

    public Processor greedy() {
        return greedy;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), left(), right(), greedy(), caseInsensitive);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        BetweenFunctionProcessor other = (BetweenFunctionProcessor) obj;
        return Objects.equals(input(), other.input())
            && Objects.equals(left(), other.left())
            && Objects.equals(right(), other.right())
            && Objects.equals(greedy(), other.greedy())
            && Objects.equals(caseInsensitive, other.caseInsensitive);
    }
}
