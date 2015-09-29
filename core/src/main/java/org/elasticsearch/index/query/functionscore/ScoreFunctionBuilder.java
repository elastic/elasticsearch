/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query.functionscore;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.lucene.search.function.WeightFactorFunction;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;

public abstract class ScoreFunctionBuilder<FB extends ScoreFunctionBuilder> implements ToXContent, NamedWriteable<FB> {

    protected Float weight;

    public abstract String getName();

    public ScoreFunctionBuilder setWeight(float weight) {
        this.weight = weight;
        return this;
    }

    public Float getWeight() {
        return weight;
    }

    protected void buildWeight(XContentBuilder builder) throws IOException {
        if (weight != null) {
            builder.field(FunctionScoreQueryParser.WEIGHT_FIELD.getPreferredName(), weight);
        }
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        buildWeight(builder);
        doXContent(builder, params);
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public String getWriteableName() {
        return getName();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        doWriteTo(out);
        if (weight == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeFloat(weight);
        }
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final FB readFrom(StreamInput in) throws IOException {
        FB scoreFunctionBuilder = doReadFrom(in);
        if (in.readBoolean()) {
            scoreFunctionBuilder.setWeight(in.readFloat());
        }
        return scoreFunctionBuilder;
    }

    protected abstract FB doReadFrom(StreamInput in) throws IOException;

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("unchecked")
        FB other = (FB) obj;
        return Objects.equals(weight, other.weight) &&
                doEquals(other);
    }

    protected abstract boolean doEquals(FB functionBuilder);

    @Override
    public final int hashCode() {
        return Objects.hash(getClass(), weight, doHashCode());
    }

    protected abstract int doHashCode();

    /**
     * Called on a data node, converts a {@link NamedWriteable} score function into its corresponding lucene function object.
     */
    public final ScoreFunction toFunction(QueryShardContext context) throws IOException {
        ScoreFunction scoreFunction = doToFunction(context);
        if (weight == null) {
            return scoreFunction;
        }
        return new WeightFactorFunction(weight, scoreFunction);
    }

    protected abstract ScoreFunction doToFunction(QueryShardContext context) throws IOException;
}
