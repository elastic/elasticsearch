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

package org.elasticsearch.transport;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.jmx.MBean;
import org.elasticsearch.jmx.ManagedAttribute;

/**
 *
 */
@MBean(objectName = "service=transport", description = "Transport")
public class TransportServiceManagement {

    private final TransportService transportService;

    @Inject
    public TransportServiceManagement(TransportService transportService) {
        this.transportService = transportService;
    }

    @ManagedAttribute(description = "Transport address published to other nodes")
    public String getPublishAddress() {
        return transportService.boundAddress().publishAddress().toString();
    }

    @ManagedAttribute(description = "Transport address bounded on")
    public String getBoundAddress() {
        return transportService.boundAddress().boundAddress().toString();
    }

    @ManagedAttribute(description = "Total number of transport requests sent")
    public long getTotalNumberOfRequests() {
        return transportService.requestIds.get();
    }
}
