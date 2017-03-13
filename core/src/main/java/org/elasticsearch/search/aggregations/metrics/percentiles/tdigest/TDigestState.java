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
package org.elasticsearch.search.aggregations.metrics.percentiles.tdigest;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.Centroid;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;

/**
 * Extension of {@link com.tdunning.math.stats.TDigest} with custom serialization.
 */
public class TDigestState extends AVLTreeDigest {

    private final double compression;

    public TDigestState(double compression) {
        super(compression);
        this.compression = compression;
    }

    @Override
    public double compression() {
        return compression;
    }

    public static void write(TDigestState state, StreamOutput out) throws IOException {
        out.writeDouble(state.compression);
        out.writeVInt(state.centroidCount());
        for (Centroid centroid : state.centroids()) {
            out.writeDouble(centroid.mean());
            out.writeVLong(centroid.count());
        }
    }

    public static TDigestState read(StreamInput in) throws IOException {
        double compression = in.readDouble();
        TDigestState state = new TDigestState(compression);
        int n = in.readVInt();
        for (int i = 0; i < n; i++) {
            state.add(in.readDouble(), in.readVInt());
        }
        return state;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj instanceof TDigestState == false) {
            return false;
        }
        TDigestState that = (TDigestState) obj;
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
