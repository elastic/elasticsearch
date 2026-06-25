/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.io.IOException;
import java.util.Objects;

/**
 * Writeable for all the change point types
 */
public interface ChangeType extends NamedWriteable, NamedXContentObject {

    // Multi change points. Before this version the result carried had a single optional change on the wire;
    // from this version it carries a (possibly null-containing) list of changes.
    public static final TransportVersion MULTI_CHANGE_POINT = TransportVersion.fromName("multi_change_point");

    int NO_CHANGE_POINT = -1;

    default int changePoint() {
        return NO_CHANGE_POINT;
    }

    default boolean isChange() {
        return changePoint() != NO_CHANGE_POINT;
    }

    default double pValue() {
        return 1.0;
    }

    default double logPValue() {
        return Math.log(pValue());
    }

    default ChangeType remapChangePoint(int changePoint) {
        return this;
    }

    abstract class AbstractChangePoint implements ChangeType {
        private final double logPValue;
        private final int changePoint;
        private final double magnitudePercent;

        protected AbstractChangePoint(double logPValue, double magnitudePercent, int changePoint) {
            this.logPValue = logPValue;
            this.magnitudePercent = magnitudePercent;
            this.changePoint = changePoint;
        }

        @Override
        public double pValue() {
            return Math.exp(logPValue);
        }

        @Override
        public double logPValue() {
            return logPValue;
        }

        public double magnitudePercent() {
            return magnitudePercent;
        }

        @Override
        public int changePoint() {
            return changePoint;
        }

        public AbstractChangePoint(StreamInput in) throws IOException {
            if (in.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                logPValue = in.readDouble();
                magnitudePercent = in.readDouble();
                changePoint = in.readVInt();
            } else {
                logPValue = Math.log(in.readDouble());
                magnitudePercent = Double.NaN;
                changePoint = in.readVInt();
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("p_value", pValue()).field("change_point", changePoint).endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                out.writeDouble(logPValue);
                out.writeDouble(magnitudePercent);
                out.writeVInt(changePoint);
            } else {
                out.writeDouble(pValue());
                out.writeVInt(changePoint);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AbstractChangePoint that = (AbstractChangePoint) o;
            return Double.compare(that.logPValue, logPValue) == 0
                && Double.compare(that.magnitudePercent, magnitudePercent) == 0
                && changePoint == that.changePoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(logPValue, magnitudePercent, changePoint);
        }
    }

    /**
     * Indicates that no change has occurred
     */
    class Indeterminable implements ChangeType {
        public static final String NAME = "indeterminable";

        private final String reason;

        public Indeterminable(String reason) {
            this.reason = reason;
        }

        public Indeterminable(StreamInput input) throws IOException {
            this.reason = input.readString();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("reason", reason).endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(reason);
        }

        @Override
        public String getName() {
            return NAME;
        }

        public String getReason() {
            return reason;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Indeterminable that = (Indeterminable) o;
            return Objects.equals(reason, that.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason);
        }
    }

    /**
     * Indicates that no change has occurred
     */
    class Stationary implements ChangeType {
        public static final String NAME = "stationary";

        public Stationary() {}

        public Stationary(StreamInput input) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(getClass());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            return obj != null && obj.getClass() == getClass();
        }
    }

    /**
     * Indicates the data has a trend
     */
    class NonStationary implements ChangeType {
        public static final String NAME = "non_stationary";
        private final double logPValue;
        private final double rValue;
        private final String trend;

        public NonStationary(double logPValue, double rValue, String trend) {
            this.logPValue = logPValue;
            this.rValue = rValue;
            this.trend = trend;
        }

        public NonStationary(StreamInput in) throws IOException {
            if (in.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                logPValue = in.readDouble();
                rValue = in.readDouble();
                trend = in.readString();
            } else {
                logPValue = Math.log(in.readDouble());
                rValue = in.readDouble();
                trend = in.readString();
            }
        }

        public String getTrend() {
            return trend;
        }

        @Override
        public double pValue() {
            return Math.exp(logPValue);
        }

        @Override
        public double logPValue() {
            return logPValue;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("p_value", pValue()).field("r_value", rValue).field("trend", trend).endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                out.writeDouble(logPValue);
                out.writeDouble(rValue);
                out.writeString(trend);
            } else {
                out.writeDouble(Math.exp(logPValue));
                out.writeDouble(rValue);
                out.writeString(trend);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NonStationary that = (NonStationary) o;
            return Double.compare(that.logPValue, logPValue) == 0
                && Double.compare(that.rValue, rValue) == 0
                && Objects.equals(trend, that.trend);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logPValue, rValue, trend);
        }
    }

    /**
     * Indicates a distribution change occurred
     */
    class DistributionChange extends AbstractChangePoint {
        public static final String NAME = "distribution_change";

        public DistributionChange(double logPValue, double magnitudePercent, int changePoint) {
            super(logPValue, magnitudePercent, changePoint);
        }

        public DistributionChange(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ChangeType remapChangePoint(int changePoint) {
            return new DistributionChange(logPValue(), magnitudePercent(), changePoint);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * Indicates a step change occurred
     */
    class StepChange extends AbstractChangePoint {
        public static final String NAME = "step_change";

        public StepChange(double logPValue, double magnitudePercent, int changePoint) {
            super(logPValue, magnitudePercent, changePoint);
        }

        public StepChange(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ChangeType remapChangePoint(int changePoint) {
            return new StepChange(logPValue(), magnitudePercent(), changePoint);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * Indicates a trend change occurred
     */
    class TrendChange implements ChangeType {
        public static final String NAME = "trend_change";
        private final double logPValue;
        private final double magnitudePercent;
        private final double rValue;
        private final int changePoint;

        public TrendChange(double logPValue, double magnitudePercent, double rValue, int changePoint) {
            this.logPValue = logPValue;
            this.magnitudePercent = magnitudePercent;
            this.rValue = rValue;
            this.changePoint = changePoint;
        }

        public TrendChange(StreamInput in) throws IOException {
            if (in.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                logPValue = in.readDouble();
                magnitudePercent = in.readDouble();
                rValue = in.readDouble();
                changePoint = in.readVInt();
            } else {
                logPValue = Math.log(in.readDouble());
                magnitudePercent = Double.NaN;
                rValue = in.readDouble();
                changePoint = in.readVInt();
            }
        }

        @Override
        public double pValue() {
            return Math.exp(logPValue);
        }

        @Override
        public double logPValue() {
            return logPValue;
        }

        public double rValue() {
            return rValue;
        }

        public double magnitudePercent() {
            return magnitudePercent;
        }

        @Override
        public int changePoint() {
            return changePoint;
        }

        @Override
        public ChangeType remapChangePoint(int changePoint) {
            return new TrendChange(logPValue, magnitudePercent, rValue, changePoint);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("p_value", pValue()).field("r_value", rValue).field("change_point", changePoint).endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                out.writeDouble(logPValue);
                out.writeDouble(magnitudePercent);
                out.writeDouble(rValue);
                out.writeVInt(changePoint);
            } else {
                out.writeDouble(Math.exp(logPValue));
                out.writeDouble(rValue);
                out.writeVInt(changePoint);
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TrendChange that = (TrendChange) o;
            return Double.compare(that.logPValue, logPValue) == 0
                && Double.compare(that.magnitudePercent, magnitudePercent) == 0
                && Double.compare(that.rValue, rValue) == 0
                && changePoint == that.changePoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(logPValue, magnitudePercent, rValue, changePoint);
        }
    }

    /**
     * Indicates a spike occurred
     */
    class Spike extends AbstractChangePoint {
        public static final String NAME = "spike";

        public Spike(double logPValue, double magnitudePercent, int changePoint) {
            super(logPValue, magnitudePercent, changePoint);
        }

        public Spike(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ChangeType remapChangePoint(int changePoint) {
            return new Spike(logPValue(), magnitudePercent(), changePoint);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * Indicates a dip occurred
     */
    class Dip extends AbstractChangePoint {
        public static final String NAME = "dip";

        public Dip(double logPValue, double magnitudePercent, int changePoint) {
            super(logPValue, magnitudePercent, changePoint);
        }

        public Dip(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ChangeType remapChangePoint(int changePoint) {
            return new Dip(logPValue(), magnitudePercent(), changePoint);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }
}
