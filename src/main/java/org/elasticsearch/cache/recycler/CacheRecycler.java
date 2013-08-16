/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import gnu.trove.map.hash.*;
import gnu.trove.set.hash.THashSet;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.recycler.QueueRecycler;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.recycler.SoftThreadLocalRecycler;
import org.elasticsearch.common.recycler.ThreadLocalRecycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;

@SuppressWarnings("unchecked")
public class CacheRecycler extends AbstractComponent {

    public final Recycler<ExtTHashMap> hashMap;
    public final Recycler<THashSet> hashSet;
    public final Recycler<ExtTDoubleObjectHashMap> doubleObjectMap;
    public final Recycler<ExtTLongObjectHashMap> longObjectMap;
    public final Recycler<TLongLongHashMap> longLongMap;
    public final Recycler<TIntIntHashMap> intIntMap;
    public final Recycler<TFloatIntHashMap> floatIntMap;
    public final Recycler<TDoubleIntHashMap> doubleIntMap;
    public final Recycler<TLongIntHashMap> longIntMap;
    public final Recycler<TObjectIntHashMap> objectIntMap;
    public final Recycler<TIntObjectHashMap> intObjectMap;
    public final Recycler<TObjectFloatHashMap> objectFloatMap;

    public void close() {
        hashMap.close();
        hashSet.close();
        doubleObjectMap.close();
        longObjectMap.close();
        longLongMap.close();
        intIntMap.close();
        floatIntMap.close();
        doubleIntMap.close();
        longIntMap.close();
        objectIntMap.close();
        intObjectMap.close();
        objectFloatMap.close();
    }

    @Inject
    public CacheRecycler(Settings settings) {
        super(settings);
        String type = settings.get("type", "soft_thread_local");
        int limit = settings.getAsInt("limit", 10);
        int smartSize = settings.getAsInt("smart_size", 1024);

        hashMap = build(type, limit, smartSize, new Recycler.C<ExtTHashMap>() {
            @Override
            public ExtTHashMap newInstance(int sizing) {
                return new ExtTHashMap(size(sizing));
            }

            @Override
            public void clear(ExtTHashMap value) {
                value.clear();
            }
        });
        hashSet = build(type, limit, smartSize, new Recycler.C<THashSet>() {
            @Override
            public THashSet newInstance(int sizing) {
                return new THashSet(size(sizing));
            }

            @Override
            public void clear(THashSet value) {
                value.clear();
            }
        });
        doubleObjectMap = build(type, limit, smartSize, new Recycler.C<ExtTDoubleObjectHashMap>() {
            @Override
            public ExtTDoubleObjectHashMap newInstance(int sizing) {
                return new ExtTDoubleObjectHashMap(size(sizing));
            }

            @Override
            public void clear(ExtTDoubleObjectHashMap value) {
                value.clear();
            }
        });
        longObjectMap = build(type, limit, smartSize, new Recycler.C<ExtTLongObjectHashMap>() {
            @Override
            public ExtTLongObjectHashMap newInstance(int sizing) {
                return new ExtTLongObjectHashMap(size(sizing));
            }

            @Override
            public void clear(ExtTLongObjectHashMap value) {
                value.clear();
            }
        });
        longLongMap = build(type, limit, smartSize, new Recycler.C<TLongLongHashMap>() {
            @Override
            public TLongLongHashMap newInstance(int sizing) {
                return new TLongLongHashMap(size(sizing));
            }

            @Override
            public void clear(TLongLongHashMap value) {
                value.clear();
            }
        });
        intIntMap = build(type, limit, smartSize, new Recycler.C<TIntIntHashMap>() {
            @Override
            public TIntIntHashMap newInstance(int sizing) {
                return new TIntIntHashMap(size(sizing));
            }

            @Override
            public void clear(TIntIntHashMap value) {
                value.clear();
            }
        });
        floatIntMap = build(type, limit, smartSize, new Recycler.C<TFloatIntHashMap>() {
            @Override
            public TFloatIntHashMap newInstance(int sizing) {
                return new TFloatIntHashMap(size(sizing));
            }

            @Override
            public void clear(TFloatIntHashMap value) {
                value.clear();
            }
        });
        doubleIntMap = build(type, limit, smartSize, new Recycler.C<TDoubleIntHashMap>() {
            @Override
            public TDoubleIntHashMap newInstance(int sizing) {
                return new TDoubleIntHashMap(size(sizing));
            }

            @Override
            public void clear(TDoubleIntHashMap value) {
                value.clear();
            }
        });
        longIntMap = build(type, limit, smartSize, new Recycler.C<TLongIntHashMap>() {
            @Override
            public TLongIntHashMap newInstance(int sizing) {
                return new TLongIntHashMap(size(sizing));
            }

            @Override
            public void clear(TLongIntHashMap value) {
                value.clear();
            }
        });
        objectIntMap = build(type, limit, smartSize, new Recycler.C<TObjectIntHashMap>() {
            @Override
            public TObjectIntHashMap newInstance(int sizing) {
                return new TObjectIntHashMap(size(sizing));
            }

            @Override
            public void clear(TObjectIntHashMap value) {
                value.clear();
            }
        });
        intObjectMap = build(type, limit, smartSize, new Recycler.C<TIntObjectHashMap>() {
            @Override
            public TIntObjectHashMap newInstance(int sizing) {
                return new TIntObjectHashMap(size(sizing));
            }

            @Override
            public void clear(TIntObjectHashMap value) {
                value.clear();
            }
        });
        objectFloatMap = build(type, limit, smartSize, new Recycler.C<TObjectFloatHashMap>() {
            @Override
            public TObjectFloatHashMap newInstance(int sizing) {
                return new TObjectFloatHashMap(size(sizing));
            }

            @Override
            public void clear(TObjectFloatHashMap value) {
                value.clear();
            }
        });
    }

    public <K, V> Recycler.V<ExtTHashMap<K, V>> hashMap(int sizing) {
        return (Recycler.V) hashMap.obtain(sizing);
    }

    public <T> Recycler.V<THashSet<T>> hashSet(int sizing) {
        return (Recycler.V) hashSet.obtain(sizing);
    }

    public <T> Recycler.V<ExtTDoubleObjectHashMap<T>> doubleObjectMap(int sizing) {
        return (Recycler.V) doubleObjectMap.obtain(sizing);
    }

    public <T> Recycler.V<ExtTLongObjectHashMap<T>> longObjectMap(int sizing) {
        return (Recycler.V) longObjectMap.obtain(sizing);
    }

    public Recycler.V<TLongLongHashMap> longLongMap(int sizing) {
        return longLongMap.obtain(sizing);
    }

    public Recycler.V<TIntIntHashMap> intIntMap(int sizing) {
        return intIntMap.obtain(sizing);
    }

    public Recycler.V<TFloatIntHashMap> floatIntMap(int sizing) {
        return floatIntMap.obtain(sizing);
    }

    public Recycler.V<TDoubleIntHashMap> doubleIntMap(int sizing) {
        return doubleIntMap.obtain(sizing);
    }

    public Recycler.V<TLongIntHashMap> longIntMap(int sizing) {
        return longIntMap.obtain(sizing);
    }

    public <T> Recycler.V<TObjectIntHashMap<T>> objectIntMap(int sizing) {
        return (Recycler.V) objectIntMap.obtain(sizing);
    }

    public <T> Recycler.V<TIntObjectHashMap<T>> intObjectMap(int sizing) {
        return (Recycler.V) intObjectMap.obtain(sizing);
    }

    public <T> Recycler.V<TObjectFloatHashMap<T>> objectFloatMap(int sizing) {
        return (Recycler.V) objectFloatMap.obtain(sizing);
    }

    static int size(int sizing) {
        return sizing > 0 ? sizing : 256;
    }

    private <T> Recycler<T> build(String type, int limit, int smartSize, Recycler.C<T> c) {
        Recycler<T> recycler;
        // default to soft_thread_local
        if (type == null || "soft_thread_local".equals(type)) {
            recycler = new SoftThreadLocalRecycler<T>(c, limit);
        } else if ("thread_local".equals(type)) {
            recycler = new ThreadLocalRecycler<T>(c, limit);
        } else if ("queue".equals(type)) {
            recycler = new QueueRecycler<T>(c);
        } else {
            throw new ElasticSearchIllegalArgumentException("no type support [" + type + "] for recycler");
        }
        if (smartSize > 0) {
            recycler = new Recycler.Sizing<T>(recycler, smartSize);
        }
        return recycler;
    }
}