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

package org.elasticsearch.tools.launchers;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Locale;

import static java.lang.System.err;
import static java.lang.System.exit;
import static java.lang.System.getProperty;
import static java.lang.System.out;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempDirectory;

/**
 * Provides a path for a temporary directory. On non-Windows OS, this will be created as a sub-directory of the default temporary directory.
 * Note that this causes the created temporary directory to be a private temporary directory.
 */
final class TempDirectory {

    /**
     * The main entry point. The exit code is 0 if we successfully created a temporary directory as a sub-directory of the default
     * temporary directory and printed the resulting path to the console.
     *
     * @param args the args to the program which should be empty
     * @throws IOException if an I/O exception occurred while creating the temporary directory
     */
    @SuppressForbidden(reason = "System#err, System#exit, System#out")
    public static void main(final String[] args) throws IOException {
        if (args.length != 0) {
            err.println(String.format(
                Locale.ROOT,
                "expected zero arguments but was %s",
                Arrays.toString(args)

            ));
            exit(1);
        }

        /*
         * On Windows, we avoid creating a unique temporary directory per invocation lest we pollute the temporary directory. On other
         * operating systems, temporary directories will be cleaned automatically via various mechanisms (e.g., systemd, or restarts).
         */
        final Path path = getProperty("os.name").startsWith("Windows")?
            createDirectories(Paths.get(getProperty("java.io.tmpdir"), "elasticsearch")):
            createTempDirectory("elasticsearch-");

        out.println(path.toString());
        exit(0);
    }

}
