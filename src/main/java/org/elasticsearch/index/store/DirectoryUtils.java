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

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.common.Nullable;

/**
 * Utils for working with {@link Directory} classes.
 */
public final class DirectoryUtils {

    private DirectoryUtils() {} // no instance

    static final <T extends Directory> Directory getLeafDirectory(FilterDirectory dir, Class<T> targetClass) {
        Directory current = dir.getDelegate();
        while (true) {
            if ((current instanceof FilterDirectory)) {
                if (targetClass != null && targetClass.isAssignableFrom(current.getClass())) {
                    break;
                }
                current = ((FilterDirectory) current).getDelegate();
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
            d = getLeafDirectory((FilterDirectory) dir, targetClass);
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
