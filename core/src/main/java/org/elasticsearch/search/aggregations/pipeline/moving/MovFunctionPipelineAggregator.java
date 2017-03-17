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

package org.elasticsearch.search.aggregations.pipeline.moving;

import org.elasticsearch.common.collect.EvictingQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.InternalSimpleValue;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.moving.models.MovModel;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MovFunctionPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;
    private final int window;
    private MovModel function;
    private final Script script;

    MovFunctionPipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, GapPolicy gapPolicy,
                                         int window, MovModel function, Script script, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.window = window;
        this.function = function;
        this.script = script;
    }

    /**
     * Read from a stream.
     */
    public MovFunctionPipelineAggregator(StreamInput in) throws IOException {
        super(in);
        formatter = in.readNamedWriteable(DocValueFormat.class);
        gapPolicy = GapPolicy.readFrom(in);
        window = in.readVInt();
        function = in.readOptionalNamedWriteable(MovModel.class);
        script = in.readOptionalWriteable(Script::new);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(formatter);
        gapPolicy.writeTo(out);
        out.writeVInt(window);
        out.writeOptionalNamedWriteable(function);
        out.writeOptionalWriteable(script);
    }

    @Override
    public String getWriteableName() {
        return MovFunctionPipelineAggregationBuilder.NAME;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>
            histo = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
            InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();
        EvictingQueue<Double> values = new EvictingQueue<>(this.window);

        // For script, if it exists
        ExecutableScript executableScript = null;

        if (script != null) {
            Map<String, Object> vars = new HashMap<>();
            ExecutableScript.Factory scriptFactory = reduceContext.scriptService()
                .compile(script, ExecutableScript.AGGS_CONTEXT);

            if (script.getParams() != null) {
                vars.putAll(script.getParams());
            }
            vars.put("values", values.toArray(new Double[values.size()]));
            executableScript = scriptFactory.newInstance(vars);
        }

        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);

            // Default is to reuse existing bucket.  Simplifies the rest of the logic,
            // since we only change newBucket if we can add to it
            Bucket newBucket = bucket;

            if (!(thisBucketValue == null || thisBucketValue.equals(Double.NaN))) {

                if (values.size() > 0) {
                    double value = Double.NaN;
                    if (executableScript != null) {
                        Object returned = executableScript.run();

                        // Scripts are allowed to return null, that will just skip
                        // the output for this bucket.  Add the existing bucket, add the value
                        // and continue to next bucket
                        if (returned == null) {
                            newBuckets.add(newBucket);
                            values.offer(thisBucketValue);
                            continue;
                        }

                        if (!(returned instanceof Number)) {
                            throw new AggregationExecutionException("Script for MovingFunction [" + name()
                                + "] must return a Number");
                        }
                        value = ((Number) returned).doubleValue();

                    } else {
                        value = function.next(values);
                    }

                    List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                        .map((p) -> (InternalAggregation) p)
                        .collect(Collectors.toList());
                    aggs.add(new InternalSimpleValue(name(), value, formatter, new ArrayList<>(), metaData()));
                    newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(),
                        new InternalAggregations(aggs));
                }
                values.offer(thisBucketValue);
            }
            newBuckets.add(newBucket);
        }
        return factory.createAggregation(newBuckets);
    }
}
