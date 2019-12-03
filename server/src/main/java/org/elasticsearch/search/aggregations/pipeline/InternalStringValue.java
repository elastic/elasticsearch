package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class InternalStringValue extends InternalAggregation {
    public static final String NAME = "string_value";

    private final String text;

    protected InternalStringValue(String name, String text, List<PipelineAggregator> pipelineAggregators, Map<String,
            Object> metaData) {
        super(name, pipelineAggregators, metaData);
        this.text = text;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not supported yet");
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return text();
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = text != null;
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? text : null);
        if (hasValue) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), text);
        }
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public String text() {
        return text;
    }
}
