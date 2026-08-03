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

    // After this version we store log probabilities and record the change percent.
    TransportVersion MULTI_CHANGE_POINT = TransportVersion.fromName("multi_change_point");

    int NO_CHANGE_POINT = -1;

    default int changePoint() {
        return NO_CHANGE_POINT;
    }

    default boolean isChange() {
        return changePoint() != NO_CHANGE_POINT;
    }

    default boolean isPointAnomaly() {
        return false;
    }

    default double pValue() {
        return 1.0;
    }

    default double logPValue() {
        return Math.log(pValue());
    }

    default ChangeType withChangePoint(int changePoint) {
        return this;
    }

    abstract class AbstractChangePoint implements ChangeType {
        private final double logPValue;
        private final int changePoint;
        private final String description;

        protected AbstractChangePoint(double logPValue, int changePoint, String description) {
            this.logPValue = logPValue;
            this.changePoint = changePoint;
            this.description = description;
        }

        @Override
        public double pValue() {
            return Math.exp(logPValue);
        }

        @Override
        public double logPValue() {
            return Math.min(logPValue, 0.0);
        }

        /** The stored, unclamped log p-value as written to the wire (see {@link #writeTo}). */
        protected double rawLogPValue() {
            return logPValue;
        }

        public String description() {
            return description;
        }

        @Override
        public int changePoint() {
            return changePoint;
        }

        public AbstractChangePoint(StreamInput in) throws IOException {
            if (in.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                logPValue = in.readDouble();
                changePoint = in.readVInt();
                description = in.readString();
            } else {
                logPValue = Math.log(in.readDouble());
                changePoint = in.readVInt();
                description = "";
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
                out.writeVInt(changePoint);
                out.writeString(description);
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
                && changePoint == that.changePoint
                && Objects.equals(that.description, description);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logPValue, changePoint, description);
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
     * Indicates a step change occurred
     */
    class StepChange extends AbstractChangePoint {
        public static final String NAME = "step_change";

        public StepChange(double logPValue, int changePoint, String description) {
            super(logPValue, changePoint, description);
        }

        public StepChange(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ChangeType withChangePoint(int changePoint) {
            return new StepChange(logPValue(), changePoint, description());
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

    /**
     * Indicates a distribution change occurred
     */
    class DistributionChange extends AbstractChangePoint {
        public static final String NAME = "distribution_change";

        public DistributionChange(double logPValue, int changePoint) {
            super(logPValue, changePoint, "");
        }

        public DistributionChange(double logPValue, int changePoint, String description) {
            super(logPValue, changePoint, description);
        }

        public DistributionChange(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ChangeType withChangePoint(int changePoint) {
            return new DistributionChange(logPValue(), changePoint, description());
        }

        @Override
        public String getName() {
            return NAME;
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
            return Math.min(logPValue, 0.0);
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
     * Indicates a trend change occurred. This is an {@link AbstractChangePoint} carrying an extra
     * {@code rValue}; only the members that differ from the base (the wire/xcontent layout, and
     * equality) are overridden.
     */
    class TrendChange extends AbstractChangePoint {
        public static final String NAME = "trend_change";
        private final double rValue;

        public TrendChange(double logPValue, double rValue, int changePoint) {
            this(logPValue, rValue, changePoint, "");
        }

        public TrendChange(double logPValue, double rValue, int changePoint, String description) {
            super(logPValue, changePoint, description);
            this.rValue = rValue;
        }

        // rValue is interleaved between logPValue and changePoint on the wire, so the base
        // AbstractChangePoint(StreamInput) reader cannot be reused (its fields would misalign). Read the
        // fields in wire order as delegating-constructor arguments - which Java evaluates left to right -
        // and forward them to the value constructor.
        public TrendChange(StreamInput in) throws IOException {
            this(
                in.getTransportVersion().supports(MULTI_CHANGE_POINT) ? in.readDouble() : Math.log(in.readDouble()),
                in.readDouble(),
                in.readVInt(),
                in.getTransportVersion().supports(MULTI_CHANGE_POINT) ? in.readString() : ""
            );
        }

        @Override
        public ChangeType withChangePoint(int changePoint) {
            return new TrendChange(rawLogPValue(), rValue, changePoint, description());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("p_value", pValue())
                .field("r_value", rValue)
                .field("change_point", changePoint())
                .endObject();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().supports(MULTI_CHANGE_POINT)) {
                out.writeDouble(rawLogPValue());
                out.writeDouble(rValue);
                out.writeVInt(changePoint());
                out.writeString(description());
            } else {
                out.writeDouble(pValue());
                out.writeDouble(rValue);
                out.writeVInt(changePoint());
            }
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public boolean equals(Object o) {
            return super.equals(o) && Double.compare(rValue, ((TrendChange) o).rValue) == 0;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), rValue);
        }
    }

    /**
     * Indicates a spike occurred
     */
    class Spike extends AbstractChangePoint {
        public static final String NAME = "spike";

        public Spike(double logPValue, int changePoint) {
            super(logPValue, changePoint, "");
        }

        public Spike(double logPValue, int changePoint, String description) {
            super(logPValue, changePoint, description);
        }

        public Spike(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public boolean isPointAnomaly() {
            return true;
        }

        @Override
        public ChangeType withChangePoint(int changePoint) {
            return new Spike(logPValue(), changePoint, description());
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

        public Dip(double logPValue, int changePoint) {
            super(logPValue, changePoint, "");
        }

        public Dip(double logPValue, int changePoint, String description) {
            super(logPValue, changePoint, description);
        }

        public Dip(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public boolean isPointAnomaly() {
            return true;
        }

        @Override
        public ChangeType withChangePoint(int changePoint) {
            return new Dip(logPValue(), changePoint, description());
        }

        @Override
        public String getName() {
            return NAME;
        }
    }
}
