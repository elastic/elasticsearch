/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceParserHelper;

import java.io.IOException;

/*
 * Wrapper for the Source config part of a composite aggregation.
 *
 * For now just wraps sources from composite aggs.
 */
public class SourceConfig implements Writeable, ToXContentObject {
    private final CompositeValuesSourceBuilder<?> sourceBuilder;

    SourceConfig(final StreamInput in) throws IOException {
        sourceBuilder = CompositeValuesSourceParserHelper.readFrom(in);
    }

    public SourceConfig(CompositeValuesSourceBuilder<?> sourceBuilder) {
        this.sourceBuilder = sourceBuilder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return CompositeValuesSourceParserHelper.toXContent(sourceBuilder, builder, params);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        CompositeValuesSourceParserHelper.writeTo(sourceBuilder, out);
    }

    public static SourceConfig fromXContent(final XContentParser parser) throws IOException {
        CompositeValuesSourceBuilder<?> builder = CompositeValuesSourceParserHelper.fromXContent(parser);
        return new SourceConfig(builder);
    }

    public CompositeValuesSourceBuilder<?> getSourceBuilder() {
        return sourceBuilder;
    }

}
