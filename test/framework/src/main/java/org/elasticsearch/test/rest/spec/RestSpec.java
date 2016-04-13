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
package org.elasticsearch.test.rest.spec;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.support.FileUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Holds the elasticsearch REST spec
 */
public class RestSpec {
    Map<String, RestApi> restApiMap = new HashMap<>();

    private RestSpec() {
    }

    void addApi(RestApi restApi) {
        restApiMap.put(restApi.getName(), restApi);
    }

    public RestApi getApi(String api) {
        return restApiMap.get(api);
    }

    public Collection<RestApi> getApis() {
        return restApiMap.values();
    }

    /**
     * Parses the complete set of REST spec available under the provided directories
     */
    public static RestSpec parseFrom(FileSystem fileSystem, String optionalPathPrefix, String... paths) throws IOException {
        RestSpec restSpec = new RestSpec();
        for (String path : paths) {
            for (Path jsonFile : FileUtils.findJsonSpec(fileSystem, optionalPathPrefix, path)) {
                try (InputStream stream = Files.newInputStream(jsonFile)) {
                    XContentParser parser = JsonXContent.jsonXContent.createParser(stream);
                    RestApi restApi = new RestApiParser().parse(parser);
                    restSpec.addApi(restApi);
                } catch (Throwable ex) {
                    throw new IOException("Can't parse rest spec file: [" + jsonFile + "]", ex);
                }
            }
        }
        return restSpec;
    }
}
