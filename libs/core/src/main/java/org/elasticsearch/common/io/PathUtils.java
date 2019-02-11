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

package org.elasticsearch.common.io;

import org.elasticsearch.common.SuppressForbidden;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;

/** 
 * Utilities for creating a Path from names,
 * or accessing the default FileSystem.
 * <p>
 * This class allows the default filesystem to
 * be changed during tests.
 */
@SuppressForbidden(reason = "accesses the default filesystem by design")
// TODO: can we move this to the .env package and make it package-private?
public final class PathUtils {
    /** no instantiation */
    private PathUtils() {}
    
    /** the actual JDK default */
    static final FileSystem ACTUAL_DEFAULT = FileSystems.getDefault();
    
    /** can be changed by tests */
    static volatile FileSystem DEFAULT = ACTUAL_DEFAULT;
    
    /** 
     * Returns a {@code Path} from name components.
     * <p>
     * This works just like {@code Paths.get()}.
     * Remember: just like {@code Paths.get()} this is NOT A STRING CONCATENATION
     * UTILITY FUNCTION.
     * <p>
     * Remember: this should almost never be used. Usually resolve
     * a path against an existing one!
     */
    public static Path get(String first, String... more) {
        return DEFAULT.getPath(first, more);
    }
    
    /** 
     * Returns a {@code Path} from a URI
     * <p>
     * This works just like {@code Paths.get()}.
     * <p>
     * Remember: this should almost never be used. Usually resolve
     * a path against an existing one!
     */
    public static Path get(URI uri) {
        if (uri.getScheme().equalsIgnoreCase("file")) {
            return DEFAULT.provider().getPath(uri);
        } else {
            return Paths.get(uri);
        }
    }

    /**
     * Tries to resolve the given path against the list of available roots.
     *
     * If path starts with one of the listed roots, it returned back by this method, otherwise null is returned.
     */
    public static Path get(Path[] roots, String path) {
        for (Path root : roots) {
            Path normalizedRoot = root.normalize();
            Path normalizedPath = normalizedRoot.resolve(path).normalize();
            if(normalizedPath.startsWith(normalizedRoot)) {
                return normalizedPath;
            }
        }
        return null;
    }

    /**
     * Tries to resolve the given file uri against the list of available roots.
     *
     * If uri starts with one of the listed roots, it returned back by this method, otherwise null is returned.
     */
    public static Path get(Path[] roots, URI uri) {
        return get(roots, PathUtils.get(uri).normalize().toString());
    }

    /**
     * Returns the default FileSystem.
     */
    public static FileSystem getDefaultFileSystem() {
        return DEFAULT;
    }
}
