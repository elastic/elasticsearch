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

package org.elasticsearch.bootstrap;

import com.google.common.io.ByteStreams;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.StringHelper;
import org.elasticsearch.env.Environment;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

/** 
 * Initializes securitymanager with necessary permissions.
 * <p>
 * We use a template file (the one we test with), and add additional 
 * permissions based on the environment (data paths, etc)
 */
class Security {
    
    /** template policy file, the one used in tests */
    static final String POLICY_RESOURCE = "security.policy";
    
    /** 
     * Initializes securitymanager for the environment
     * Can only happen once!
     */
    static void configure(Environment environment) throws IOException {
        // init lucene random seed. it will use /dev/urandom where available.
        StringHelper.randomId();
        InputStream config = Security.class.getResourceAsStream(POLICY_RESOURCE);
        if (config == null) {
            throw new NoSuchFileException(POLICY_RESOURCE);
        }
        Path newConfig = processTemplate(config, environment);
        System.setProperty("java.security.policy", newConfig.toString());
        System.setSecurityManager(new SecurityManager());
        IOUtils.deleteFilesIgnoringExceptions(newConfig); // TODO: maybe log something if it fails?
    }
   
    // package-private for testing
    static Path processTemplate(InputStream template, Environment environment) throws IOException {
        Path processed = Files.createTempFile(null, null);
        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(processed))) {
            // copy the template as-is.
            try (InputStream in = new BufferedInputStream(template)) {
                ByteStreams.copy(in, output);
            }

            //  all policy files are UTF-8:
            //  https://docs.oracle.com/javase/7/docs/technotes/guides/security/PolicyFiles.html
            try (Writer writer = new OutputStreamWriter(output, StandardCharsets.UTF_8)) {
                writer.write(System.lineSeparator());
                writer.write("grant {");
                writer.write(System.lineSeparator());

                // add permissions for all configured paths.
                // TODO: improve test infra so we can reduce permissions where read/write
                // is not really needed...
                addPath(writer, environment.homeFile(), "read,readlink,write,delete");
                addPath(writer, environment.configFile(), "read,readlink,write,delete");
                addPath(writer, environment.logsFile(), "read,readlink,write,delete");
                addPath(writer, environment.pluginsFile(), "read,readlink,write,delete");
                for (Path path : environment.dataFiles()) {
                    addPath(writer, path, "read,readlink,write,delete");
                }
                for (Path path : environment.dataWithClusterFiles()) {
                    addPath(writer, path, "read,readlink,write,delete");
                }

                writer.write("};");
                writer.write(System.lineSeparator());
            }
        }
        return processed;
    }
    
    static void addPath(Writer writer, Path path, String permissions) throws IOException {
        // paths may not exist yet
        Files.createDirectories(path);
        // add each path twice: once for itself, again for files underneath it
        writer.write("permission java.io.FilePermission \"" + encode(path) + "\", \"" + permissions + "\";");
        writer.write(System.lineSeparator());
        writer.write("permission java.io.FilePermission \"" + encode(path) + "${/}-\", \"" + permissions + "\";");
        writer.write(System.lineSeparator());
    }
    
    // Any backslashes in paths must be escaped, because it is the escape character when parsing.
    // See "Note Regarding File Path Specifications on Windows Systems".
    // https://docs.oracle.com/javase/7/docs/technotes/guides/security/PolicyFiles.html
    static String encode(Path path) {
        return path.toString().replace("\\", "\\\\");
    }
}
