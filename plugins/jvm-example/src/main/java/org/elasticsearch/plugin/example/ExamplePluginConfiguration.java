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

package org.elasticsearch.plugin.example;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.newBufferedReader;
import static org.elasticsearch.common.io.Streams.copyToString;

/**
 * Example configuration.
 */
public class ExamplePluginConfiguration {
    private String test = "not set in config";

    @Inject
    public ExamplePluginConfiguration(Environment env) throws IOException {
        // The directory part of the location matches the artifactId of this plugin
        Path configFile = env.configFile().resolve("jvm-example/example.yaml");
        String contents = copyToString(newBufferedReader(configFile, UTF_8));
        XContentParser parser = YamlXContent.yamlXContent.createParser(contents);

        String currentFieldName = null;
        XContentParser.Token token = parser.nextToken();
        assert token == XContentParser.Token.START_OBJECT;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if ("test".equals(currentFieldName)) {
                    test = parser.text();
                } else {
                    throw new ElasticsearchParseException("Unrecognized config key: {}", currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("Unrecognized config key: {}", currentFieldName);
            }
        }
    }

    public String getTestConfig() {
        return test;
    }
}
