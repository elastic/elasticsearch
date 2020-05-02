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
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * A request to create an component template.
 */
public class PutComponentTemplateRequest extends TimedRequest implements ToXContentObject {

    private String name;

    private String cause = "";

    private boolean create;

    private ComponentTemplate componentTemplate;

    /**
     * Sets the name of the component template.
     */
    public PutComponentTemplateRequest name(String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name cannot be null or empty");
        }
        this.name = name;
        return this;
    }

    /**
     * The name of the component template.
     */
    public String name() {
        return this.name;
    }

    /**
     * Set to {@code true} to force only creation, not an update of an component template. If it already
     * exists, it will fail with an {@link IllegalArgumentException}.
     */
    public PutComponentTemplateRequest create(boolean create) {
        this.create = create;
        return this;
    }

    public boolean create() {
        return create;
    }

    /**
     * The component template to create.
     */
    public PutComponentTemplateRequest componentTemplate(ComponentTemplate componentTemplate) {
        this.componentTemplate = componentTemplate;
        return this;
    }

    /**
     * The cause for this component template creation.
     */
    public PutComponentTemplateRequest cause(String cause) {
        this.cause = cause;
        return this;
    }

    public String cause() {
        return this.cause;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (componentTemplate != null) {
            componentTemplate.toXContent(builder, params);
        }
        return builder;
    }
}
