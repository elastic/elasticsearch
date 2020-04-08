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

package org.elasticsearch.client.indices;

import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;

/**
 * A request to read the content of index templates
 */
public class GetIndexTemplateV2Request implements Validatable {

    private final String name;

    private TimeValue masterNodeTimeout = TimedRequest.DEFAULT_MASTER_NODE_TIMEOUT;
    private boolean local = false;

    /**
     * Create a request to read the content of index template. If no template name is provided, all templates will
     * be read
     *
     * @param name the name of template to read
     */
    public GetIndexTemplateV2Request(String name) {
        this.name = name;
    }

    /**
     * @return the name of index template this request is requesting
     */
    public String name() {
        return name;
    }

    /**
     * @return the timeout for waiting for the master node to respond
     */
    public TimeValue getMasterNodeTimeout() {
        return masterNodeTimeout;
    }

    public void setMasterNodeTimeout(@Nullable TimeValue masterNodeTimeout) {
        this.masterNodeTimeout = masterNodeTimeout;
    }

    public void setMasterNodeTimeout(String masterNodeTimeout) {
        final TimeValue timeValue = TimeValue.parseTimeValue(masterNodeTimeout, getClass().getSimpleName() + ".masterNodeTimeout");
        setMasterNodeTimeout(timeValue);
    }

    /**
     * @return true if this request is to read from the local cluster state, rather than the master node - false otherwise
     */
    public boolean isLocal() {
        return local;
    }

    public void setLocal(boolean local) {
        this.local = local;
    }
}
