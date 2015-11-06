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

package org.elasticsearch;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 */
@SuppressForbidden(reason = "needs JarFile to read the manifest")
public class Build {

    public static final Build CURRENT;

    static {
        String shortHash = "Unknown";
        String date = "Unknown";

        String path = Build.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        try {
            JarFile jar = new JarFile(path);
            Manifest manifest = jar.getManifest();
            shortHash = manifest.getMainAttributes().getValue("Change");
            date = manifest.getMainAttributes().getValue("Build-Date");
        } catch (IOException e) {
            // just ignore...
        }

        CURRENT = new Build(shortHash, date);
    }

    private String shortHash;
    private String date;

    Build(String shortHash, String date) {
        this.shortHash = shortHash;
        this.date = date;
    }

    public String shortHash() {
        return shortHash;
    }

    public String date() {
        return date;
    }

    public static Build readBuild(StreamInput in) throws IOException {
        String hash = in.readString();
        String date = in.readString();
        return new Build(hash, date);
    }

    public static void writeBuild(Build build, StreamOutput out) throws IOException {
        out.writeString(build.shortHash());
        out.writeString(build.date());
    }

    @Override
    public String toString() {
        return "[" + shortHash + "][" + date + "]";
    }
}
