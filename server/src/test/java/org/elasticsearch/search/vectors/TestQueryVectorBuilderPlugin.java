/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A SearchPlugin to exercise query vector builder
 */
public class TestQueryVectorBuilderPlugin implements SearchPlugin {

    public static class TestQueryVectorBuilder implements QueryVectorBuilder {
        private static final String NAME = "test_query_vector_builder";

        private static final ParseField QUERY_VECTOR = new ParseField("query_vector");

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<TestQueryVectorBuilder, Void> PARSER = new ConstructingObjectParser<>(
            NAME + "_parser",
            true,
            a -> new TestQueryVectorBuilder((List<Float>) a[0])
        );

        static {
            PARSER.declareFloatArray(ConstructingObjectParser.constructorArg(), QUERY_VECTOR);
        }

        private final List<Float> vectorToBuild;

        public TestQueryVectorBuilder(List<Float> vectorToBuild) {
            this.vectorToBuild = vectorToBuild;
        }

        public TestQueryVectorBuilder(float[] expected) {
            this.vectorToBuild = new ArrayList<>(expected.length);
            for (float f : expected) {
                vectorToBuild.add(f);
            }
        }

        TestQueryVectorBuilder(StreamInput in) throws IOException {
            this.vectorToBuild = in.readCollectionAsList(StreamInput::readFloat);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field(QUERY_VECTOR.getPreferredName(), vectorToBuild).endObject();
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.current();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(vectorToBuild, StreamOutput::writeFloat);
        }

        @Override
        public void buildVector(Client client, ActionListener<float[]> listener) {
            float[] response = new float[vectorToBuild.size()];
            int i = 0;
            for (Float f : vectorToBuild) {
                response[i++] = f;
            }
            listener.onResponse(response);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestQueryVectorBuilder that = (TestQueryVectorBuilder) o;
            return Objects.equals(vectorToBuild, that.vectorToBuild);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vectorToBuild);
        }
    }

    @Override
    public List<QueryVectorBuilderSpec<?>> getQueryVectorBuilders() {
        return List.of(
            new QueryVectorBuilderSpec<>(TestQueryVectorBuilder.NAME, TestQueryVectorBuilder::new, TestQueryVectorBuilder.PARSER)
        );
    }
}
