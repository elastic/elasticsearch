package org.elasticsearch.search.aggregations.metrics;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;

public class LegacyTDigestState extends AVLTreeDigest {
    private final double compression;

    public LegacyTDigestState(double compression) {
        super(compression);
        this.compression = compression;
    }

    @Override
    public double compression() {
        return compression;
    }

    public static void write(LegacyTDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);
        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }
    }

    public static LegacyTDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        LegacyTDigestState state = new LegacyTDigestState(compression);
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof LegacyTDigestState == false) {
            return false;
        }
        LegacyTDigestState that = (LegacyTDigestState) obj;
        if (compression != that.compression) {
            return false;
        }
        Iterator<? extends Centroid> thisCentroids = centroids().iterator();
        Iterator<? extends Centroid> thatCentroids = that.centroids().iterator();
        while (thisCentroids.hasNext()) {
            if (thatCentroids.hasNext() == false) {
                return false;
            }
            Centroid thisNext = thisCentroids.next();
            Centroid thatNext = thatCentroids.next();
            if (thisNext.mean() != thatNext.mean() || thisNext.count() != thatNext.count()) {
                return false;
            }
        }
        return thatCentroids.hasNext() == false;
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + Double.hashCode(compression);
        for (Centroid centroid : centroids()) {
            h = 31 * h + Double.hashCode(centroid.mean());
            h = 31 * h + centroid.count();
        }
        return h;
    }
}
