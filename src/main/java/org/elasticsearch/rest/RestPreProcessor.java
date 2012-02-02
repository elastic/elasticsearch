/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.rest;

/**
 * Rest pre processor allowing to pre process REST requests.
 * <p/>
 * Experimental interface.
 */
public interface RestPreProcessor {

    /**
     * Optionally, the order the processor will work on. Execution is done from lowest value to highest.
     * It is a good practice to allow to configure this for the relevant processor.
     */
    int order();

    /**
     * Should this processor also process external (non REST) requests, like plugin site requests.
     */
    boolean handleExternal();

    /**
     * Process the request, returning <tt>false</tt> if no further processing should be done. Note,
     * make sure to send a response if returning <tt>false</tt>, otherwise, no response will be sent.
     */
    boolean process(RestRequest request, RestChannel channel);
}
