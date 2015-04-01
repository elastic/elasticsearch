package org.elasticsearch.search.aggregations.reducers.acf;

import org.apache.commons.lang3.ArrayUtils;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.reducers.Reducer;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalAcfValues extends InternalNumericMetricsAggregation.MultiValue {

    public final static Type TYPE = new Type("acf_values");

    public final static AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalAcfValues readResult(StreamInput in) throws IOException {
            InternalAcfValues result = new InternalAcfValues();
            result.readFrom(in);
            return result;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    private double[] values;

    InternalAcfValues() {} // for serialization

    @Override
    public double value(String name) {
        return values[Integer.parseInt(name)];
    }

    public double[] values() {
        return values;
    }

    public InternalAcfValues(String name, double[] values, @Nullable ValueFormatter formatter, List<Reducer> reducers, Map<String, Object> metaData) {
        super(name, reducers, metaData);
        this.valueFormatter = formatter;
        this.values = values;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public InternalMax doReduce(ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    protected void doReadFrom(StreamInput in) throws IOException {
        valueFormatter = ValueFormatterStreams.readOptional(in);
        values = in.readDoubleArray();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(valueFormatter, out);
        out.writeDoubleArray(values);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        //nocommit this all needs a closer look
        // TODO
        builder.array(CommonFields.VALUES, ArrayUtils.toObject(values));
        if (valueFormatter != null) {
            String[] stringValues = new String[values.length];
            for (int i = 0; i < values.length; i++) {
                stringValues[i] = valueFormatter.format(values[i]);
            }
            builder.array(CommonFields.VALUES_AS_STRING, stringValues);
        }
        return builder;
    }
}
