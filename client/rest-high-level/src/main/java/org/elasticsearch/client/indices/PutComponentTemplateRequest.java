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

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.client.TimedRequest;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * A request to create an component template.
 */
public class PutComponentTemplateRequest extends TimedRequest implements ToXContentFragment {

    private String name;

    private String cause = "";

    private boolean create;

    private ComponentTemplate componentTemplate;

    /**
     * Sets the name of the component template.
     */
    public PutComponentTemplateRequest name(String name) {
        if (name == null) {
            throw new IllegalArgumentException("Name cannot be null");
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

    /**
     * The template source definition.
     */
    public PutComponentTemplateRequest source(XContentBuilder templateBuilder) {
        try {
            return source(BytesReference.bytes(templateBuilder), templateBuilder.contentType());
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to build json for template request", e);
        }
    }

    /**
     * The template source definition.
     */
    public PutComponentTemplateRequest source(String templateSource, XContentType xContentType) {
        try (XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, templateSource)) {
            return componentTemplate(ComponentTemplate.parse(parser));
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * The template source definition.
     */
    public PutComponentTemplateRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * The template source definition.
     */
    public PutComponentTemplateRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        try (XContentParser parser = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, source, offset, length)) {
            return componentTemplate(ComponentTemplate.parse(parser));
        } catch (IOException e) {
            throw new ElasticsearchParseException("Failed to parse content to map", e);
        }
    }

    /**
     * The template source definition.
     */
    public PutComponentTemplateRequest source(BytesReference source, XContentType xContentType) {
        BytesRef ref = source.toBytesRef();
        return source(ref.bytes, ref.offset, ref.length, xContentType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (componentTemplate != null) {
            componentTemplate.toXContent(builder, params);
        }
        return builder;
    }

    @Override
    public boolean isFragment() {
        return false;
    }
}
