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
package org.elasticsearch.gradle;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ClassPathUtils {

    public static Path getJar(Class<?> theClass) {
        URL location = theClass.getProtectionDomain().getCodeSource().getLocation();
        if (location.getProtocol().equals("file") == false) {
            throw new IllegalArgumentException(
                "Unexpected location for " + theClass.getName() + ": "+ location
            );
        }
        final Path path;
        try {
            path = Paths.get(location.toURI());
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
        if (Files.exists(path) == false) {
            throw new AssertionError("Bath to class source does not exist: " + path);
        }
        return path;
    }

}
