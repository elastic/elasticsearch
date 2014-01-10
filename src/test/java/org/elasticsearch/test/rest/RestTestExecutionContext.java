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
package org.elasticsearch.test.rest;

import com.google.common.collect.Maps;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.rest.client.RestClient;
import org.elasticsearch.test.rest.client.RestException;
import org.elasticsearch.test.rest.client.RestResponse;
import org.elasticsearch.test.rest.spec.RestSpec;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Execution context passed across the REST tests.
 * Holds the REST client used to communicate with elasticsearch.
 * Caches the last obtained test response and allows to stash part of it within variables
 * that can be used as input values in following requests.
 */
public class RestTestExecutionContext implements Closeable {

    private static final ESLogger logger = Loggers.getLogger(RestTestExecutionContext.class);

    private final RestClient restClient;

    private final String esVersion;

    private final Map<String, Object> stash = Maps.newHashMap();

    private RestResponse response;

    public RestTestExecutionContext(String host, int port, RestSpec restSpec) throws RestException, IOException {

        this.restClient = new RestClient(host, port, restSpec);
        this.esVersion = restClient.getEsVersion();
    }

    /**
     * Calls an elasticsearch api with the parameters and request body provided as arguments.
     * Saves the obtained response in the execution context.
     * @throws RestException if the returned status code is non ok
     */
    public RestResponse callApi(String apiName, Map<String, String> params, String body) throws IOException, RestException  {
        //makes a copy of the parameters before modifying them for this specific request
        HashMap<String, String> requestParams = Maps.newHashMap(params);
        for (Map.Entry<String, String> entry : requestParams.entrySet()) {
            if (isStashed(entry.getValue())) {
                entry.setValue(unstash(entry.getValue()).toString());
            }
        }
        try {
            return response = callApiInternal(apiName, requestParams, body);
        } catch(RestException e) {
            response = e.restResponse();
            throw e;
        }
    }

    /**
     * Calls an elasticsearch api internally without saving the obtained response in the context.
     * Useful for internal calls (e.g. delete index during teardown)
     * @throws RestException if the returned status code is non ok
     */
    public RestResponse callApiInternal(String apiName, String... params) throws IOException, RestException {
        return restClient.callApi(apiName, params);
    }

    private RestResponse callApiInternal(String apiName, Map<String, String> params, String body) throws IOException, RestException  {
        return restClient.callApi(apiName, params, body);
    }

    /**
     * Extracts a specific value from the last saved response
     */
    public Object response(String path) throws IOException {
        return response.evaluate(path);
    }

    /**
     * Clears the last obtained response and the stashed fields
     */
    public void clear() {
        logger.debug("resetting response and stash");
        response = null;
        stash.clear();
    }

    /**
     * Tells whether a particular value needs to be looked up in the stash
     * The stash contains fields eventually extracted from previous responses that can be reused
     * as arguments for following requests (e.g. scroll_id)
     */
    public boolean isStashed(Object key) {
        if (key == null) {
            return false;
        }
        String stashKey = key.toString();
        return Strings.hasLength(stashKey) && stashKey.startsWith("$");
    }

    /**
     * Extracts a value from the current stash
     * The stash contains fields eventually extracted from previous responses that can be reused
     * as arguments for following requests (e.g. scroll_id)
     */
    public Object unstash(String value) {
        Object stashedValue = stash.get(value.substring(1));
        if (stashedValue == null) {
            throw new IllegalArgumentException("stashed value not found for key [" + value + "]");
        }
        return stashedValue;
    }

    /**
     * Allows to saved a specific field in the stash as key-value pair
     */
    public void stash(String key, Object value) {
        logger.debug("stashing [{}]=[{}]", key, value);
        Object old = stash.put(key, value);
        if (old != null && old != value) {
            logger.trace("replaced stashed value [{}] with same key [{}]", old, key);
        }
    }

    /**
     * Returns the current es version as a string
     */
    public String esVersion() {
        return esVersion;
    }

    /**
     * Closes the execution context and releases the underlying resources
     */
    public void close() {
        this.restClient.close();
    }
}
