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

package org.elasticsearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

import java.io.IOException;
import java.util.Collection;

/**
 * Utils for working with {@link Directory} classes.
 */
public final class DirectoryUtils {

    private DirectoryUtils() {} // no instance

    private static final Directory getLeafDirectory(FilterDirectory dir) {
        Directory current = dir.getDelegate();
        while ((current instanceof FilterDirectory)) {
            current = ((FilterDirectory) current).getDelegate();
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
        if (targetClass.isAssignableFrom(d.getClass())) {
            return targetClass.cast(d);
        } else {
            return defaultValue;
        }
    }
    

}
