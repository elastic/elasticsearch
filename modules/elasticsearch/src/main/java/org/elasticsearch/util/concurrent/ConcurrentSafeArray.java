/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.util.concurrent;

import org.elasticsearch.util.SafeArray;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A concurrent version of {@link SafeArray}.
 *
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public class ConcurrentSafeArray<T> implements SafeArray<T> {

    private final ArrayList<T> list = new ArrayList<T>();

    private final ReadWriteLock rwl = new ReentrantReadWriteLock();

    @Override public T get(int index) {
        rwl.readLock().lock();
        try {
            return list.get(index);
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public int size() {
        rwl.readLock().lock();
        try {
            return list.size();
        } finally {
            rwl.readLock().unlock();
        }
    }

    @Override public void add(T value) {
        rwl.writeLock().lock();
        try {
            list.add(value);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override public void add(int index, T value) {
        rwl.writeLock().lock();
        try {
            list.add(index, value);
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override public void clear() {
        rwl.writeLock().lock();
        try {
            list.clear();
        } finally {
            rwl.writeLock().unlock();
        }
    }

    @Override public boolean forEach(Procedure<T> procedure) {
        rwl.readLock().lock();
        try {
            for (int i = 0; i < list.size(); i++) {
                if (!procedure.execute(list.get(i))) {
                    return false;
                }
            }
            return true;
        } finally {
            rwl.readLock().unlock();
        }
    }
}
