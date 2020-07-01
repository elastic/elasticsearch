/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata.plain;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.xpack.spatial.index.fielddata.LeafPointFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiPointValues;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class AbstractLeafPointFieldData implements LeafPointFieldData {

    @Override
    public final SortedBinaryDocValues getBytesValues() {
        MultiPointValues in = getPointValues();
        return new SortingBinaryDocValues() {

            final List<CharSequence> list = new ArrayList<>();

            @Override
            public boolean advanceExact(int docID) throws IOException {
                if (in.advanceExact(docID) == false) {
                    return false;
                }
                list.clear();
                for (int i = 0, count = in.docValueCount(); i < count; ++i) {
                    list.add(in.nextValue().toString());
                }
                count = list.size();
                grow();
                for (int i = 0; i < count; ++i) {
                    final CharSequence s = list.get(i);
                    values[i].copyChars(s);
                }
                sort();
                return true;
            }

        };
    }

    @Override
    public final CartesianPoints getScriptValues() {
        return new CartesianPoints(getPointValues());
    }

    public static LeafPointFieldData empty(final int maxDoc) {
        return new AbstractLeafPointFieldData() {

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }

            @Override
            public void close() {
            }

            @Override
            public MultiPointValues getPointValues() {
                return new MultiPointValues() {
                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        return false;
                    }

                    @Override
                    public int docValueCount() {
                        return 0;
                    }

                    @Override
                    public CartesianPoint nextValue() throws IOException {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public static final class CartesianPoints extends ScriptDocValues<CartesianPoint> {

        private final MultiPointValues in;
        private CartesianPoint[] values = new CartesianPoint[0];
        private int count;

        public CartesianPoints(MultiPointValues in) {
            this.in = in;
        }

        @Override
        public void setNextDocId(int docId) throws IOException {
            if (in.advanceExact(docId)) {
                resize(in.docValueCount());
                for (int i = 0; i < count; i++) {
                    CartesianPoint point = in.nextValue();
                    values[i] = new CartesianPoint(point.getX(), point.getY());
                }
            } else {
                resize(0);
            }
        }

        /**
         * Set the {@link #size()} and ensure that the {@link #values} array can
         * store at least that many entries.
         */
        protected void resize(int newSize) {
            count = newSize;
            if (newSize > values.length) {
                int oldLength = values.length;
                values = ArrayUtil.grow(values, count);
                for (int i = oldLength; i < values.length; ++i) {
                    values[i] = new CartesianPoint();
                }
            }
        }

        public CartesianPoint getValue() {
            return get(0);
        }

        public float getX() {
            return getValue().getX();
        }

        public float[] getXs() {
            float[] xs = new float[size()];
            for (int i = 0; i < size(); i++) {
                xs[i] = get(i).getX();
            }
            return xs;
        }

        public float getY() {
            return getValue().getY();
        }

        public float[] getYs() {
            float[] ys = new float[size()];
            for (int i = 0; i < size(); i++) {
                ys[i] = get(i).getY();
            }
            return ys;
        }

        @Override
        public CartesianPoint get(int index) {
            if (count == 0) {
                throw new IllegalStateException("A document doesn't have a value for a field! " +
                    "Use doc[<field>].size()==0 to check if a document is missing a field!");
            }
            final CartesianPoint point = values[index];
            return new CartesianPoint(point.getX(), point.getY());
        }

        @Override
        public int size() {
            return count;
        }

        public double distance(double x, double y) {
            CartesianPoint point = getValue();
            return CartesianPoint.cartesianDistance(x, y, point.getX(), point.getY());
        }
    }
}
