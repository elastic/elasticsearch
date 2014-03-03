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

package org.elasticsearch.cache.recycler;

import org.elasticsearch.common.recycler.AbstractRecyclerC;
import org.elasticsearch.common.recycler.NoneRecycler;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.recycler.Recycler.V;
import org.elasticsearch.common.util.BigArrays;

// Should implement PageCacheRecycler once that is made into an interface
public class NonPageCacheRecyclerService {

    private final Recycler<byte[]> byteRecycler;
    private final Recycler<int[]> intRecycler;
    private final Recycler<long[]> longRecycler;
    private final Recycler<float[]> floatRecycler;
    private final Recycler<double[]> doubleRecycler;
    private final Recycler<Object[]> objectRecycler;

    public NonPageCacheRecyclerService() {
        Recycler.C<byte[]> rcb = new NoneRecyclingByteFactory();
        this.byteRecycler = new NoneRecycler<byte[]>(rcb);
        Recycler.C<int[]> rci = new NoneRecyclingIntFactory();
        this.intRecycler = new NoneRecycler<int[]>(rci);
        Recycler.C<long[]> rcl = new NoneRecyclingLongFactory();
        this.longRecycler = new NoneRecycler<long[]>(rcl);
        Recycler.C<float[]> rcf = new NoneRecyclingFloatFactory();
        this.floatRecycler = new NoneRecycler<float[]>(rcf);
        Recycler.C<double[]> rcd = new NoneRecyclingDoubleFactory();
        this.doubleRecycler = new NoneRecycler<double[]>(rcd);
        Recycler.C<Object[]> rco = new NoneRecyclingObjectFactory();
        this.objectRecycler = new NoneRecycler<Object[]>(rco);
    }
    
    public V<byte[]> bytePage(boolean clear) {
        return byteRecycler.obtain(BigArrays.BYTE_PAGE_SIZE);
    }

    public V<int[]> intPage(boolean clear) {
        return intRecycler.obtain(BigArrays.BYTE_PAGE_SIZE);
    }

    public V<long[]> longPage(boolean clear) {
        return longRecycler.obtain(BigArrays.BYTE_PAGE_SIZE);
    }

    public V<float[]> floatPage(boolean clear) {
        return floatRecycler.obtain(BigArrays.BYTE_PAGE_SIZE);
    }

    public V<double[]> doublePage(boolean clear) {
        return doubleRecycler.obtain(BigArrays.BYTE_PAGE_SIZE);
    }

    public V<Object[]> objectPage() {
        return objectRecycler.obtain(BigArrays.BYTE_PAGE_SIZE);
    }

    public void close() {
        byteRecycler.close();
        intRecycler.close();
        longRecycler.close();
        floatRecycler.close();
        doubleRecycler.close();
        objectRecycler.close();
    }

    private static class NoneRecyclingByteFactory extends AbstractRecyclerC<byte[]> {
        @Override
        public byte[] newInstance(int sizing) {
            return new byte[sizing];
        }

        @Override
        public void recycle(byte[] value) {
            // don't actually recycle: drop for GC
        }
    }

    private static class NoneRecyclingIntFactory extends AbstractRecyclerC<int[]> {
        @Override
        public int[] newInstance(int sizing) {
            return new int[sizing];
        }

        @Override
        public void recycle(int[] value) {
            // don't actually recycle: drop for GC
        }
    }

    private static class NoneRecyclingLongFactory extends AbstractRecyclerC<long[]> {
        @Override
        public long[] newInstance(int sizing) {
            return new long[sizing];
        }

        @Override
        public void recycle(long[] value) {
            // don't actually recycle: drop for GC
        }
    }

    private static class NoneRecyclingFloatFactory extends AbstractRecyclerC<float[]> {
        @Override
        public float[] newInstance(int sizing) {
            return new float[sizing];
        }

        @Override
        public void recycle(float[] value) {
            // don't actually recycle: drop for GC
        }
    }

    private static class NoneRecyclingDoubleFactory extends AbstractRecyclerC<double[]> {
        @Override
        public double[] newInstance(int sizing) {
            return new double[sizing];
        }

        @Override
        public void recycle(double[] value) {
            // don't actually recycle: drop for GC
        }
    }

    private static class NoneRecyclingObjectFactory extends AbstractRecyclerC<Object[]> {
        @Override
        public Object[] newInstance(int sizing) {
            return new Object[sizing];
        }

        @Override
        public void recycle(Object[] value) {
            // don't actually recycle: drop for GC
        }
    }

}
