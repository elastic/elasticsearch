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

package org.elasticsearch.cloud.gce;

import com.google.api.services.compute.model.Instance;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.LifecycleComponent;

import java.util.Collection;

/**
 *
 */
public interface GceComputeService extends LifecycleComponent<GceComputeService> {
    static final public class Fields {
        public static final String PROJECT = "cloud.gce.project_id";
        public static final String ZONE = "cloud.gce.zone";
        public static final String REFRESH = "cloud.gce.refresh_interval";
        public static final String TAGS = "discovery.gce.tags";
        public static final String VERSION = "Elasticsearch/GceCloud/1.0";
    }

    public Collection<Instance> instances();
}
