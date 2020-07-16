/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
    private final boolean isCaseSensitive;

    public BetweenFunctionProcessor(Processor input, Processor left, Processor right, Processor greedy, boolean isCaseSensitive) {
        this.input = input;
        this.left = left;
        this.right = right;
        this.greedy = greedy;
        this.isCaseSensitive = isCaseSensitive;
    }

    public BetweenFunctionProcessor(StreamInput in) throws IOException {
        input = in.readNamedWriteable(Processor.class);
        left = in.readNamedWriteable(Processor.class);
        right = in.readNamedWriteable(Processor.class);
        greedy = in.readNamedWriteable(Processor.class);
        isCaseSensitive = in.readBoolean();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(input);
        out.writeNamedWriteable(left);
        out.writeNamedWriteable(right);
        out.writeNamedWriteable(greedy);
        out.writeBoolean(isCaseSensitive);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Object process(Object o) {
        return doProcess(input.process(o), left.process(o), right.process(o), greedy.process(o), isCaseSensitive());
    }

    public static Object doProcess(Object input, Object left, Object right, Object greedy, boolean isCaseSensitive) {
        if (input == null || left == null || right == null || greedy == null) {
            return null;
        }

        Check.isString(input);
        Check.isString(left);
        Check.isString(right);
        Check.isBoolean(greedy);

        String str = input.toString();
        String strRight = right.toString();
        String strLeft = left.toString();
        boolean bGreedy = (Boolean) greedy;
        return StringUtils.between(str, strLeft, strRight, bGreedy, isCaseSensitive);
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

    protected boolean isCaseSensitive() {
        return isCaseSensitive;
    }

    @Override
    public int hashCode() {
        return Objects.hash(input(), left(), right(), greedy(), isCaseSensitive);
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
                && Objects.equals(isCaseSensitive(), other.isCaseSensitive());
    }
}
