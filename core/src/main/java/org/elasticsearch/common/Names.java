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

package org.elasticsearch.common;

import com.google.common.base.Charsets;
import org.elasticsearch.common.io.FileSystemUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public abstract class Names {

    public static String randomNodeName(InputStream namesFile) {
        try {
            List<String> names = new ArrayList<>();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(namesFile, Charsets.UTF_8))) {
                String name = reader.readLine();
                while (name != null) {
                    names.add(name);
                    name = reader.readLine();
                }
            }
            int index = ((ThreadLocalRandom.current().nextInt(names.size())) % names.size());
            return names.get(index);
        } catch (IOException e) {
            throw new RuntimeException("Could not read node names list", e);
        }
    }

    private Names() {}
}
