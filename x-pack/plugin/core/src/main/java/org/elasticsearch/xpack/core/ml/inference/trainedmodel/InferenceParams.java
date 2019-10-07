package org.elasticsearch.xpack.core.ml.inference.trainedmodel;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class InferenceParams implements ToXContentObject, Writeable {

    public static ParseField NUM_TOP_CLASSES = new ParseField("num_top_classes");

    private final int numTopClasses;

    public InferenceParams(Integer numTopClasses) {
        this.numTopClasses = numTopClasses == null ? 0 : numTopClasses;
    }

    public InferenceParams(StreamInput in) throws IOException {
        this.numTopClasses = in.readInt();
    }

    public int getNumTopClasses() {
        return numTopClasses;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(numTopClasses);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InferenceParams that = (InferenceParams) o;
        return Objects.equals(numTopClasses, that.numTopClasses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numTopClasses);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (numTopClasses != 0) {
            builder.field(NUM_TOP_CLASSES.getPreferredName(), numTopClasses);
        }
        builder.endObject();
        return builder;
    }
}
