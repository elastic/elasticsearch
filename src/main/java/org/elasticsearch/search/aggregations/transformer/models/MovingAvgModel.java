package org.elasticsearch.search.aggregations.transformer.models;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;

public final class MovingAvgModel {

    public static enum Weighting {
        simple((byte) 0), linear((byte) 1), ewma((byte) 2);

        private byte id;

        private Weighting(byte id) {
            this.id = id;
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static Weighting readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (Weighting weight : values()) {
                if (id == weight.id) {
                    return weight;
                }
            }
            throw new IllegalStateException("Unknown Weighting with id [" + id + "]");
        }
    }

    /**
     * Calculate the moving average, according to the weighting method chosen (simple, linear, ewma)
     *
     * @param values Ringbuffer containing the current window of values
     * @param <T>    Type T extending Number
     * @return       Returns a double containing the moving avg for the window
     */
    public static <T extends Number> double next(Collection<T> values, Weighting weight) {
        switch (weight) {
            case simple:
                return simpleMovingAvg(values);
            case linear:
                return linearMovingAvg(values);
            case ewma:
                return ewmaMovingAvg(values);
            default:
                return simpleMovingAvg(values);
        }
    }

    private static <T extends Number> double simpleMovingAvg(Collection<T> values) {
        double avg = 0;
        for (T v : values) {
            avg += v.doubleValue();
        }
        return avg / values.size();
    }

    private static <T extends Number> double linearMovingAvg(Collection<T> values) {
        double avg = 0;
        long totalWeight = 1;
        long current = 1;

        for (T v : values) {
            avg += v.doubleValue() * current;
            totalWeight += current;
            current += 1;
        }
        return avg / totalWeight;
    }

    private static <T extends Number> double ewmaMovingAvg(Collection<T> values) {
        double avg = 0;
        double alpha = 2.0 / (double) (values.size() + 1);  //TODO expose alpha or period as a param to the user
        boolean first = true;

        for (T v : values) {
            if (first) {
                avg = v.doubleValue();
                first = false;
            } else {
                avg = (v.doubleValue() * alpha) + (avg * (1 - alpha));
            }
        }
        return avg;
    }
}
