package org.elasticsearch.search.aggregations.reducers.movavg;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

public final class MovAvgModel {

    public static enum Weighting {
        SIMPLE((byte) 0, "simple"), LINEAR((byte) 1, "linear"), SINGLE_EXP((byte) 2, "single_exp"), DOUBLE_EXP((byte) 3, "double_exp");

        public static Weighting parse(SearchContext context, String text) {
            Weighting result = null;
            for (Weighting policy : values()) {
                if (policy.parseField.match(text)) {
                    if (result == null) {
                        result = policy;
                    } else {
                        throw new ElasticsearchIllegalStateException("Text can be parsed to 2 different weighting policies: text=[" + text
                                + "], " + "policies=" + Arrays.asList(result, policy));
                    }
                }
            }
            if (result == null) {
                final List<String> validNames = new ArrayList<>();
                for (Weighting policy : values()) {
                    validNames.add(policy.getName());
                }
                throw new SearchParseException(context, "Invalid weighting policy: [" + text + "], accepted values: " + validNames);
            }
            return result;
        }

        private final byte id;
        private final ParseField parseField;

        private Weighting(byte id, String name) {
            this.id = id;
            this.parseField = new ParseField(name);
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(id);
        }

        public static Weighting readFrom(StreamInput in) throws IOException {
            byte id = in.readByte();
            for (Weighting weighting : values()) {
                if (id == weighting.id) {
                    return weighting;
                }
            }
            throw new IllegalStateException("Unknown GapPolicy with id [" + id + "]");
        }

        public String getName() {
            return parseField.getPreferredName();
        }
    }

    /**
     * Calculate the moving average, according to the weighting method chosen (simple, linear, ewma)
     *
     * @param values Ringbuffer containing the current window of values
     * @param <T>    Type T extending Number
     * @return       Returns a double containing the moving avg for the window
     */
    public static <T extends Number> double next(Collection<T> values, Weighting weight, Map<String, Object> settings) {
        double alpha;
        double beta;

        switch (weight) {
            case SIMPLE:
                return simpleMovingAvg(values);
            case LINEAR:
                return linearMovingAvg(values);
            case SINGLE_EXP:
                alpha = (double)settings.get("alpha");
                return singleExponential(values, alpha);
            case DOUBLE_EXP:
                alpha = (double)settings.get("alpha");
                beta = (double)settings.get("beta");

                return (doubleExponential(values, 1, alpha, beta)).get(0);
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

    private static <T extends Number> double singleExponential(Collection<T> values, double alpha) {
        double avg = 0;
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

    private static <T extends Number> List<Double> doubleExponential(Collection<T> values, int numForecasts, double alpha, double beta) {
        // Smoothed value
        double s = 0;
        double last_s = 0;

        // Trend value
        double b = 0;
        double last_b = 0;

        int counter = 0;

        //TODO bail if too few values

        T last;
        for (T v : values) {
            last = v;
            if (counter == 1) {
                s = v.doubleValue();
                b = v.doubleValue() - last.doubleValue();
            } else {
                s = alpha * v.doubleValue() + (1.0d - alpha) * (last_s + last_b);
                b = beta * (s - last_s) + (1 - beta) * last_b;
            }

            counter += 1;
            last_s = s;
            last_b = b;
        }

        List<Double> forecastValues = new ArrayList<>(numForecasts);
        for (int i = 0; i < numForecasts; i++) {
            forecastValues.add(s + (i * b));
        }

        return forecastValues;
    }
}
