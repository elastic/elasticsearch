/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;

import java.util.List;
import java.util.Map;

public class SystemDataStreamDescriptor {

    private final String dataStreamName;
    private final String description;
    private final Type type;
    private final ComposableIndexTemplate composableIndexTemplate;
    private final Map<String, ComponentTemplate> componentTemplates;
    private final List<String> allowedElasticProductOrigins;

    public SystemDataStreamDescriptor(String dataStreamName, String description, Type type,
                                      ComposableIndexTemplate composableIndexTemplate, Map<String, ComponentTemplate> componentTemplates,
                                      List<String> allowedElasticProductOrigins) {
        // TODO validation and javadocs
        this.dataStreamName = dataStreamName;
        this.description = description;
        this.type = type;
        this.composableIndexTemplate = composableIndexTemplate;
        this.componentTemplates = Map.copyOf(componentTemplates);
        this.allowedElasticProductOrigins = allowedElasticProductOrigins;
    }

    public String getDataStreamName() {
        return dataStreamName;
    }

    public String getDescription() {
        return description;
    }

    public ComposableIndexTemplate getComposableIndexTemplate() {
        return composableIndexTemplate;
    }

    public boolean isExternal() {
        return type == Type.EXTERNAL;
    }

    public String getBackingIndexPattern() {
        return DataStream.BACKING_INDEX_PREFIX + getDataStreamName() + "-*";
    }

    public List<String> getAllowedElasticProductOrigins() {
        return allowedElasticProductOrigins;
    }

    public Map<String, ComponentTemplate> getComponentTemplates() {
        return componentTemplates;
    }

    public enum Type {
        INTERNAL,
        EXTERNAL
    }
}
