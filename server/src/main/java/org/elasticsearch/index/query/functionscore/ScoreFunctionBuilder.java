/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;
import java.util.Objects;

public abstract class ScoreFunctionBuilder<FB extends ScoreFunctionBuilder<FB>> implements ToXContentFragment, NamedWriteable {

    private Float weight;

    /**
     * Standard empty constructor.
     */
    public ScoreFunctionBuilder() {
    }

    /**
     * Read from a stream.
     */
    public ScoreFunctionBuilder(StreamInput in) throws IOException {
        weight = checkWeight(in.readOptionalFloat());
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalFloat(weight);
        doWriteTo(out);
    }

    /**
     * Write the subclass's components into the stream.
     */
    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    /**
     * The name of this score function.
     */
    public abstract String getName();

    /**
     * Set the weight applied to the function before combining.
     */
    @SuppressWarnings("unchecked")
    public final FB setWeight(float weight) {
        this.weight = checkWeight(weight);
        return (FB) this;
    }

    private Float checkWeight(Float weight) {
        if (weight != null && Float.compare(weight, 0) < 0) {
            throw new IllegalArgumentException("[weight] cannot be negative for a filtering function");
        }
        return weight;
    }

    /**
     * The weight applied to the function before combining.
     */
    public final Float getWeight() {
        return weight;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (weight != null) {
            builder.field(FunctionScoreQueryBuilder.WEIGHT_FIELD.getPreferredName(), weight);
        }
        doXContent(builder, params);
        return builder;
    }

    /**
     * Convert this subclass's data into XContent.
     */
    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public String getWriteableName() {
        return getName();
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        FB other = (FB) obj;
        return Objects.equals(weight, other.getWeight()) && doEquals(other);
    }

    /**
     * Check that two instances of the same subclass of ScoreFunctionBuilder are equal. Implementers don't need to check any fields in
     * ScoreFunctionBuilder, just fields that they define.
     */
    protected abstract boolean doEquals(FB functionBuilder);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), weight, doHashCode());
    }

    /**
     * Hashcode for fields defined in this subclass of ScoreFunctionBuilder. Implementers should ignore fields defined in
     * ScoreFunctionBuilder because they will already be in the hashCode.
     */
    protected abstract int doHashCode();

    /**
     * Called on a data node, converts this ScoreFunctionBuilder into its corresponding Lucene function object.
     */
    public final ScoreFunction toFunction(SearchExecutionContext context) throws IOException {
        ScoreFunction scoreFunction = doToFunction(context);
        if (weight == null) {
            return scoreFunction;
        }
        return new WeightFactorFunction(weight, scoreFunction);
    }

    /**
     * Build the Lucene ScoreFunction for this builder. Implementers should ignore things defined in ScoreFunctionBuilder like weight as
     * they will be handled by the function that calls this one.
     */
    protected abstract ScoreFunction doToFunction(SearchExecutionContext context) throws IOException;
}
