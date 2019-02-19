package org.elasticsearch.xpack.core.dataframe.transform;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.io.IOException;
import java.util.Locale;

public enum DataFrameTransformState implements Writeable {
    STARTED, INDEXING, STOPPING, STOPPED, ABORTING, FAILED;

    public static DataFrameTransformState fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public static DataFrameTransformState fromStream(StreamInput in) throws IOException {
        return in.readEnum(DataFrameTransformState.class);
    }

    public static DataFrameTransformState fromIndexerState(IndexerState state) {
        return fromString(state.toString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        DataFrameTransformState state = this;
        out.writeEnum(state);
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    public IndexerState toIndexerState() {
        return this == FAILED ? IndexerState.STOPPED : IndexerState.fromString(this.toString());
    }
}
