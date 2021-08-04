/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Object representing the extent of a geometry object within a {@link TriangleTreeWriter}.
 */
class Extent {

    public int top;
    public int bottom;
    public int negLeft;
    public int negRight;
    public int posLeft;
    public int posRight;

    private static final byte NONE_SET = 0;
    private static final byte POSITIVE_SET = 1;
    private static final byte NEGATIVE_SET = 2;
    private static final byte CROSSES_LAT_AXIS = 3;
    private static final byte ALL_SET = 4;


    Extent() {
        this.top = Integer.MIN_VALUE;
        this.bottom = Integer.MAX_VALUE;
        this.negLeft = Integer.MAX_VALUE;
        this.negRight = Integer.MIN_VALUE;
        this.posLeft = Integer.MAX_VALUE;
        this.posRight = Integer.MIN_VALUE;
    }

    Extent(int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        this.top = top;
        this.bottom = bottom;
        this.negLeft = negLeft;
        this.negRight = negRight;
        this.posLeft = posLeft;
        this.posRight = posRight;
    }

    public void reset(int top, int bottom, int negLeft, int negRight, int posLeft, int posRight) {
        this.top = top;
        this.bottom = bottom;
        this.negLeft = negLeft;
        this.negRight = negRight;
        this.posLeft = posLeft;
        this.posRight = posRight;
    }

    /**
     * Adds the extent of two points representing a bounding box's bottom-left
     * and top-right points. The bounding box must not cross the dateline.
     *
     * @param bottomLeftX the bottom-left x-coordinate
     * @param bottomLeftY the bottom-left y-coordinate
     * @param topRightX   the top-right x-coordinate
     * @param topRightY   the top-right y-coordinate
     */
    public void addRectangle(int bottomLeftX, int bottomLeftY, int topRightX, int topRightY) {
        assert bottomLeftX <= topRightX;
        assert bottomLeftY <= topRightY;
        this.bottom = Math.min(this.bottom, bottomLeftY);
        this.top = Math.max(this.top, topRightY);
        if (bottomLeftX < 0 && topRightX < 0) {
            this.negLeft = Math.min(this.negLeft, bottomLeftX);
            this.negRight = Math.max(this.negRight, topRightX);
        } else if (bottomLeftX < 0) {
            this.negLeft = Math.min(this.negLeft, bottomLeftX);
            this.posRight = Math.max(this.posRight, topRightX);
            // this signal the extent cannot be wrapped around the dateline
            this.negRight = 0;
            this.posLeft = 0;
        } else {
            this.posLeft = Math.min(this.posLeft, bottomLeftX);
            this.posRight = Math.max(this.posRight, topRightX);
        }
    }

    static void readFromCompressed(ByteArrayDataInput input, Extent extent) {
        try {
            final int top = CodecUtil.readBEInt(input);
            final int bottom = Math.toIntExact(top - input.readVLong());
            final int negLeft;
            final int negRight;
            final int posLeft;
            final int posRight;
            byte type = input.readByte();
            switch (type) {
                case NONE_SET:
                    negLeft = Integer.MAX_VALUE;
                    negRight = Integer.MIN_VALUE;
                    posLeft = Integer.MAX_VALUE;
                    posRight = Integer.MIN_VALUE;
                    break;
                case POSITIVE_SET:
                    posLeft = input.readVInt();
                    posRight = Math.toIntExact(input.readVLong() + posLeft);
                    negLeft = Integer.MAX_VALUE;
                    negRight = Integer.MIN_VALUE;
                    break;
                case NEGATIVE_SET:
                    negRight = -input.readVInt();
                    negLeft = Math.toIntExact(negRight - input.readVLong());
                    posLeft = Integer.MAX_VALUE;
                    posRight = Integer.MIN_VALUE;
                    break;
                case CROSSES_LAT_AXIS:
                    posRight = input.readVInt();
                    negLeft = -input.readVInt();
                    posLeft = 0;
                    negRight = 0;
                    break;
                case ALL_SET:
                    posLeft = input.readVInt();
                    posRight = Math.toIntExact(input.readVLong() + posLeft);
                    negRight = -input.readVInt();
                    negLeft = Math.toIntExact(negRight - input.readVLong());
                    break;
                default:
                    throw new IllegalArgumentException("invalid extent values-set byte read [" + type + "]");
            }
            extent.reset(top, bottom, negLeft, negRight, posLeft, posRight);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    void writeCompressed(ByteBuffersDataOutput output) throws IOException {
        CodecUtil.writeBEInt(output, this.top);
        output.writeVLong((long) this.top - this.bottom);
        byte type;
        if (this.negLeft == Integer.MAX_VALUE && this.negRight == Integer.MIN_VALUE) {
            if (this.posLeft == Integer.MAX_VALUE && this.posRight == Integer.MIN_VALUE) {
                type = NONE_SET;
            } else {
                type = POSITIVE_SET;
            }
        } else if (this.posLeft == Integer.MAX_VALUE && this.posRight == Integer.MIN_VALUE) {
            type = NEGATIVE_SET;
        } else {
            if (posLeft == 0 && negRight == 0) {
                type = CROSSES_LAT_AXIS;
            } else {
                type = ALL_SET;
            }
        }
        output.writeByte(type);
        switch (type) {
            case NONE_SET : break;
            case POSITIVE_SET:
                output.writeVInt(this.posLeft);
                output.writeVLong((long) this.posRight - this.posLeft);
                break;
            case NEGATIVE_SET:
                output.writeVInt(-this.negRight);
                output.writeVLong((long) this.negRight - this.negLeft);
                break;
            case CROSSES_LAT_AXIS:
                output.writeVInt(this.posRight);
                output.writeVInt(-this.negLeft);
                break;
            case ALL_SET:
                output.writeVInt(this.posLeft);
                output.writeVLong((long) this.posRight - this.posLeft);
                output.writeVInt(-this.negRight);
                output.writeVLong((long) this.negRight - this.negLeft);
                break;
            default:
                throw new IllegalArgumentException("invalid extent values-set byte read [" + type + "]");
        }
    }

    /**
     * calculates the extent of a point, which is the point itself.
     * @param x the x-coordinate of the point
     * @param y the y-coordinate of the point
     * @return the extent of the point
     */
    public static Extent fromPoint(int x, int y) {
        return new Extent(y, y,
            x < 0 ? x : Integer.MAX_VALUE,
            x < 0 ? x : Integer.MIN_VALUE,
            x >= 0 ? x : Integer.MAX_VALUE,
            x >= 0 ? x : Integer.MIN_VALUE);
    }

    /**
     * calculates the extent of two points representing a bounding box's bottom-left
     * and top-right points. It is important that these points accurately represent the
     * bottom-left and top-right of the extent since there is no validation being done.
     *
     * @param bottomLeftX the bottom-left x-coordinate
     * @param bottomLeftY the bottom-left y-coordinate
     * @param topRightX   the top-right x-coordinate
     * @param topRightY   the top-right y-coordinate
     * @return the extent of the two points
     */
    static Extent fromPoints(int bottomLeftX, int bottomLeftY, int topRightX, int topRightY) {
        int negLeft = Integer.MAX_VALUE;
        int negRight = Integer.MIN_VALUE;
        int posLeft = Integer.MAX_VALUE;
        int posRight = Integer.MIN_VALUE;
        if (bottomLeftX < 0 && topRightX < 0) {
            negLeft = bottomLeftX;
            negRight = topRightX;
        } else if (bottomLeftX < 0) {
            negLeft = bottomLeftX;
            posRight = topRightX;
            // this signal the extent cannot be wrapped around the dateline
            negRight = 0;
            posLeft = 0;
        } else {
            posLeft = bottomLeftX;
            posRight = topRightX;
        }
        return new Extent(topRightY, bottomLeftY, negLeft, negRight, posLeft, posRight);
    }

    /**
     * @return the minimum y-coordinate of the extent
     */
    public int minY() {
        return bottom;
    }

    /**
     * @return the maximum y-coordinate of the extent
     */
    public int maxY() {
        return top;
    }

    /**
     * @return the absolute minimum x-coordinate of the extent, whether it is positive or negative.
     */
    public int minX() {
        return Math.min(negLeft, posLeft);
    }

    /**
     * @return the absolute maximum x-coordinate of the extent, whether it is positive or negative.
     */
    public int maxX() {
        return Math.max(negRight, posRight);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Extent extent = (Extent) o;
        return top == extent.top &&
            bottom == extent.bottom &&
            negLeft == extent.negLeft &&
            negRight == extent.negRight &&
            posLeft == extent.posLeft &&
            posRight == extent.posRight;
    }

    @Override
    public int hashCode() {
        return Objects.hash(top, bottom, negLeft, negRight, posLeft, posRight);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("[");
        builder.append("top = " + top + ", ");
        builder.append("bottom = " + bottom + ", ");
        builder.append("negLeft = " + negLeft + ", ");
        builder.append("negRight = " + negRight + ", ");
        builder.append("posLeft = " + posLeft + ", ");
        builder.append("posRight = " + posRight + "]");
        return builder.toString();
    }
}
