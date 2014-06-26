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

package org.elasticsearch.index.store;

import org.apache.lucene.store.*;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;

/**
 * Utils for working with {@link Directory} classes.
 */
public final class DirectoryUtils {

    static {
        assert Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9 : "Remove the special case for NRTCachingDirectory - it implements FilterDirectory in 4.10";
    }

    private DirectoryUtils() {} // no instance

    /**
     * Try and extract a store directory out of a directory, tries to take into
     * account the fact that a directory is a filter directory, and/or a compound dir.
     */
    @Nullable
    public static Store.StoreDirectory getStoreDirectory(Directory dir) {
        Directory current = dir;
        while (true) {
            if (current instanceof Store.StoreDirectory) {
                return (Store.StoreDirectory) current;
            }
            if (current instanceof FilterDirectory) {
                current = ((FilterDirectory) current).getDelegate();
            } else if (current instanceof CompoundFileDirectory) {
                current = ((CompoundFileDirectory) current).getDirectory();
            } else {
                return null;
            }
        }
    }

    static final Directory getLeafDirectory(FilterDirectory dir) {
        Directory current = dir.getDelegate();
        while (true) {
            if ((current instanceof FilterDirectory)) {
                current = ((FilterDirectory) current).getDelegate();
            } else if (current instanceof NRTCachingDirectory) { // remove this when we upgrade to Lucene 4.10
                current = ((NRTCachingDirectory) current).getDelegate();
            } else {
                break;
            }
        }
        return current;
    }

    /**
     * Tries to extract the leaf of the {@link Directory} if the directory is a {@link FilterDirectory} and cast
     * it to the given target class or returns <code>null</code> if the leaf is not assignable to the target class.
     * If the given {@link Directory} is a concrete directory it will treated as a leaf and the above applies.
     */
    public static <T extends Directory> T getLeaf(Directory dir, Class<T> targetClass) {
        return getLeaf(dir, targetClass, null);
    }
    /**
     * Tries to extract the leaf of the {@link Directory} if the directory is a {@link FilterDirectory} and cast
     * it to the given target class or returns the given default value, if the leaf is not assignable to the target class.
     * If the given {@link Directory} is a concrete directory it will treated as a leaf and the above applies.
     */
    public static <T extends Directory> T getLeaf(Directory dir, Class<T> targetClass, T defaultValue) {
        Directory d = dir;
        if (dir instanceof FilterDirectory) {
            d = getLeafDirectory((FilterDirectory) dir);
        }
        if (d instanceof FileSwitchDirectory) {
            T leaf = getLeaf(((FileSwitchDirectory) d).getPrimaryDir(), targetClass);
            if (leaf == null) {
                d = getLeaf(((FileSwitchDirectory) d).getSecondaryDir(), targetClass, defaultValue);
            } else {
                d = leaf;
            }
        }

        if (d != null && targetClass.isAssignableFrom(d.getClass())) {
            return targetClass.cast(d);
        } else {
            return defaultValue;
        }
    }
    

}
