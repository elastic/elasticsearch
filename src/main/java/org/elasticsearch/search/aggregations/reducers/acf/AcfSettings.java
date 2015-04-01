package org.elasticsearch.search.aggregations.reducers.acf;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

public class AcfSettings implements Streamable {

    public enum Mode {
        STATISTICAL, SIGNAL
    }

    private boolean zeroPad = true;
    private boolean normalize = true;
    private boolean zeroMean = true;
    private int window = 5;


    public AcfSettings() {}

    public AcfSettings(int window, boolean zeroPad, boolean zeroMean, boolean normalize) {
        this.window = window;
        this.zeroPad = zeroPad;
        this.zeroMean = zeroMean;
        this.normalize = normalize;
    }

    public boolean isZeroPad() {
        return zeroPad;
    }

    public void setZeroPad(boolean zeroPad) {
        this.zeroPad = zeroPad;
    }

    public boolean isNormalize() {
        return normalize;
    }

    public void setNormalize(boolean normalize) {
        this.normalize = normalize;
    }

    public boolean isZeroMean() {
        return zeroMean;
    }

    public void setZeroMean(boolean zeroMean) {
        this.zeroMean = zeroMean;
    }

    public int getWindow() {
        return window;
    }

    public void setWindow(int window) {
        this.window = window;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(window);
        out.writeBoolean(zeroPad);
        out.writeBoolean(zeroMean);
        out.writeBoolean(normalize);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        window = in.readVInt();
        zeroPad = in.readBoolean();
        zeroMean = in.readBoolean();
        normalize = in.readBoolean();
    }

    // nocommit
    // TODO this seems a lot more convenient, but should I rely on the Serializable interface instead?
    // See AcfReducer.doReadFrom for usage
    public static AcfSettings doReadFrom(StreamInput in) throws IOException {
        return new AcfSettings(in.readVInt(), in.readBoolean(), in.readBoolean(), in.readBoolean());
    }



}
