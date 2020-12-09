/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.painless.action.PainlessContextInfo;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class ContextApiSpecGenerator {
    public static void main(String[] args) throws IOException {
        List<PainlessContextInfo> contexts = ContextGeneratorCommon.getContextInfos();
        ContextGeneratorCommon.PainlessInfos infos = new ContextGeneratorCommon.PainlessInfos(contexts);
        Path rootDir = resetRootDir();
        Path json = rootDir.resolve("painless-common.json");
        try (PrintStream jsonStream = new PrintStream(
             Files.newOutputStream(json, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
             false, StandardCharsets.UTF_8.name())) {

            XContentBuilder builder = XContentFactory.jsonBuilder(jsonStream);
            builder.startObject();
            builder.field(PainlessContextInfo.CLASSES.getPreferredName(), infos.common);
            builder.endObject();
            builder.flush();
        }

        for (PainlessInfoJson.Context context : infos.contexts) {
            json = rootDir.resolve("painless-" + context.getName() + ".json");
            try (PrintStream jsonStream = new PrintStream(
                Files.newOutputStream(json, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                false, StandardCharsets.UTF_8.name())) {

                XContentBuilder builder = XContentFactory.jsonBuilder(jsonStream);
                context.toXContent(builder, null);
                builder.flush();
            }
        }
    }

    @SuppressForbidden(reason = "resolve context api directory with environment")
    private static Path resetRootDir() throws IOException {
        Path rootDir = PathUtils.get("./src/main/generated/whitelist-json");
        IOUtils.rm(rootDir);
        Files.createDirectories(rootDir);

        return rootDir;
    }
}
