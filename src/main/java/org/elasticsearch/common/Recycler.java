package org.elasticsearch.common;

import gnu.trove.map.hash.TByteIntHashMap;
import gnu.trove.map.hash.TDoubleIntHashMap;
import gnu.trove.map.hash.TFloatIntHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import gnu.trove.map.hash.TLongLongHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.map.hash.TShortIntHashMap;
import gnu.trove.set.hash.THashSet;

import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;

public interface Recycler {

    public abstract void clear();

    public abstract <K, V> ExtTHashMap<K, V> popHashMap();

    public abstract void pushHashMap(ExtTHashMap map);

    public abstract <T> THashSet<T> popHashSet();

    public abstract void pushHashSet(THashSet map);

    public abstract <T> ExtTDoubleObjectHashMap<T> popDoubleObjectMap();

    public abstract void pushDoubleObjectMap(ExtTDoubleObjectHashMap map);

    public abstract <T> ExtTLongObjectHashMap<T> popLongObjectMap();

    public abstract void pushLongObjectMap(ExtTLongObjectHashMap map);

    public abstract TLongLongHashMap popLongLongMap();

    public abstract void pushLongLongMap(TLongLongHashMap map);

    public abstract TIntIntHashMap popIntIntMap();

    public abstract void pushIntIntMap(TIntIntHashMap map);

    public abstract TFloatIntHashMap popFloatIntMap();

    public abstract void pushFloatIntMap(TFloatIntHashMap map);

    public abstract TDoubleIntHashMap popDoubleIntMap();

    public abstract void pushDoubleIntMap(TDoubleIntHashMap map);

    public abstract TByteIntHashMap popByteIntMap();

    public abstract void pushByteIntMap(TByteIntHashMap map);

    public abstract TShortIntHashMap popShortIntMap();

    public abstract void pushShortIntMap(TShortIntHashMap map);

    public abstract TLongIntHashMap popLongIntMap();

    public abstract void pushLongIntMap(TLongIntHashMap map);

    public abstract <T> TObjectIntHashMap<T> popObjectIntMap();

    public abstract <T> void pushObjectIntMap(TObjectIntHashMap<T> map);

    public abstract <T> TIntObjectHashMap<T> popIntObjectMap();

    public abstract <T> void pushIntObjectMap(TIntObjectHashMap<T> map);

    public abstract <T> TObjectFloatHashMap<T> popObjectFloatMap();

    public abstract <T> void pushObjectFloatMap(TObjectFloatHashMap<T> map);

    public abstract Object[] popObjectArray(int size);

    public abstract void pushObjectArray(Object[] objects);

    public abstract int[] popIntArray(int size);

    public abstract int[] popIntArray(int size, int sentinal);

    public abstract void pushIntArray(int[] ints);

    public abstract void pushIntArray(int[] ints, int sentinal);

}