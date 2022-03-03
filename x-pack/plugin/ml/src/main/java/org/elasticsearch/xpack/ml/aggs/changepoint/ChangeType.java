/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

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

    default int changePoint() {
        return -1;
    }

    default double pValue() {
        return 1.0;
    }

    abstract class AbstractChangePoint implements ChangeType {
        private final double pValue;
        private final int changePoint;

        protected AbstractChangePoint(double pValue, int changePoint) {
            this.pValue = pValue;
            this.changePoint = changePoint;
        }

        @Override
        public double pValue() {
            return pValue;
        }

        @Override
        public int changePoint() {
            return changePoint;
        }

        public AbstractChangePoint(StreamInput in) throws IOException {
            pValue = in.readDouble();
            changePoint = in.readVInt();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("p_value", pValue).field("change_point", changePoint).endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(pValue);
            out.writeVInt(changePoint);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AbstractChangePoint that = (AbstractChangePoint) o;
            return Double.compare(that.pValue, pValue) == 0 && changePoint == that.changePoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pValue, changePoint);
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

        public StepChange(double pValue, int changePoint) {
            super(pValue, changePoint);
        }

        public StepChange(StreamInput in) throws IOException {
            super(in);
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

        public DistributionChange(double pValue, int changePoint) {
            super(pValue, changePoint);
        }

        public DistributionChange(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getName() {
            return NAME;
        }

    }

    /**
     * Indicates a trend change occurred
     */
    class NonStationary implements ChangeType {
        public static final String NAME = "non_stationary";
        private final double pValue;
        private final double rValue;
        private final String trend;

        public NonStationary(double pValue, double rValue, String trend) {
            this.pValue = pValue;
            this.rValue = rValue;
            this.trend = trend;
        }

        public NonStationary(StreamInput in) throws IOException {
            pValue = in.readDouble();
            rValue = in.readDouble();
            trend = in.readString();
        }

        public String getTrend() {
            return trend;
        }

        @Override
        public double pValue() {
            return pValue;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("p_value", pValue).field("r_value", rValue).field("trend", trend).endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(pValue);
            out.writeDouble(rValue);
            out.writeString(trend);
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
            return Double.compare(that.pValue, pValue) == 0
                && Double.compare(that.rValue, rValue) == 0
                && Objects.equals(trend, that.trend);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pValue, rValue, trend);
        }
    }

    /**
     * Indicates a trend change occurred
     */
    class TrendChange implements ChangeType {
        public static final String NAME = "trend_change";
        private final double pValue;
        private final double rValue;
        private final int changePoint;

        public TrendChange(double pValue, double rValue, int changePoint) {
            this.pValue = pValue;
            this.rValue = rValue;
            this.changePoint = changePoint;
        }

        public TrendChange(StreamInput in) throws IOException {
            pValue = in.readDouble();
            rValue = in.readDouble();
            changePoint = in.readVInt();
        }

        @Override
        public double pValue() {
            return pValue;
        }

        @Override
        public int changePoint() {
            return changePoint;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("p_value", pValue).field("r_value", pValue).field("change_point", changePoint).endObject();
        }

        @Override
        public String getWriteableName() {
            return getName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(pValue);
            out.writeDouble(rValue);
            out.writeVInt(changePoint);
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
            return Double.compare(that.pValue, pValue) == 0 && Double.compare(that.rValue, rValue) == 0 && changePoint == that.changePoint;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pValue, rValue, changePoint);
        }
    }

    /**
     * Indicates a spike occurred
     */
    class Spike extends AbstractChangePoint {
        public static final String NAME = "spike";

        public Spike(double pValue, int changePoint) {
            super(pValue, changePoint);
        }

        public Spike(StreamInput in) throws IOException {
            super(in);
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

        public Dip(double pValue, int changePoint) {
            super(pValue, changePoint);
        }

        public Dip(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getName() {
            return NAME;
        }
    }

}
