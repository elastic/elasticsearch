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

package org.elasticsearch.rest.action.ingest;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.ingest.core.ConfigurationUtils;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestFilter;
import org.elasticsearch.rest.RestFilterChain;
import org.elasticsearch.rest.RestRequest;

public class IngestRestFilter extends RestFilter {

    @Inject
    public IngestRestFilter(RestController controller) {
        controller.registerFilter(this);
    }

    @Override
    public void process(RestRequest request, RestChannel channel, RestFilterChain filterChain) throws Exception {
        if (request.hasParam(ConfigurationUtils.PIPELINE_ID_PARAM)) {
            request.putInContext(ConfigurationUtils.PIPELINE_ID_PARAM_CONTEXT_KEY, request.param(ConfigurationUtils.PIPELINE_ID_PARAM));
        }
        filterChain.continueProcessing(request, channel);
    }
}
