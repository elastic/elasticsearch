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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.support.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Holds the elasticsearch REST spec
 */
public class RestSpec {
    Map<String, RestApi> restApiMap = Maps.newHashMap();

    private RestSpec() {
    }

    void addApi(RestApi restApi) {
        if ("info".equals(restApi.getName())) {
            //info and ping should really be two different api in the rest spec
            //info (GET|HEAD /) needs to be manually split into 1) info: GET /  2) ping: HEAD /
            restApiMap.put("info", new RestApi(restApi, "info", "GET"));
            restApiMap.put("ping", new RestApi(restApi, "ping", "HEAD"));
        } else if ("get".equals(restApi.getName())) {
            //get_source endpoint shouldn't be present in the rest spec for the get api
            //as get_source is already a separate api
            List<String> paths = Lists.newArrayList();
            for (String path : restApi.getPaths()) {
                if (!path.endsWith("/_source")) {
                    paths.add(path);
                }
            }
            restApiMap.put(restApi.getName(), new RestApi(restApi, paths));
        } else {
            restApiMap.put(restApi.getName(), restApi);
        }
    }

    public RestApi getApi(String api) {
        return restApiMap.get(api);
    }

    /**
     * Parses the complete set of REST spec available under the provided directories
     */
    public static RestSpec parseFrom(String optionalPathPrefix, String... paths) throws IOException {
        RestSpec restSpec = new RestSpec();
        for (String path : paths) {
            for (File jsonFile : FileUtils.findJsonSpec(optionalPathPrefix, path)) {
                try {
                    XContentParser parser = JsonXContent.jsonXContent.createParser(new FileInputStream(jsonFile));
                    RestApi restApi = new RestApiParser().parse(parser);
                    restSpec.addApi(restApi);
                } catch (IOException ex) {
                    throw new IOException("Can't parse rest spec file: [" + jsonFile + "]", ex);
                }
            }
        }
        return restSpec;
    }
}
