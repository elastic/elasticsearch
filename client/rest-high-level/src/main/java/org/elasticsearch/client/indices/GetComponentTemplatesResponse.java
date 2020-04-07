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

import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


public class GetComponentTemplatesResponse {

    public static final ParseField NAME = new ParseField("name");
    public static final ParseField COMPONENT_TEMPLATES = new ParseField("component_templates");
    public static final ParseField COMPONENT_TEMPLATE = new ParseField("component_template");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Map<String, ComponentTemplate>, Void> PARSER =
        new ConstructingObjectParser<>("component_templates", false,
            a -> ((List<NamedComponentTemplate>) a[0]).stream().collect(Collectors.toMap(n -> n.name, n -> n.componentTemplate,
                (n1, n2) -> n1, LinkedHashMap::new)));

    private static final ConstructingObjectParser<NamedComponentTemplate, Void> INNER_PARSER =
        new ConstructingObjectParser<>("named_component_template", false,
            a -> new NamedComponentTemplate((String) a[0], (ComponentTemplate) a[1]));

    static {
        INNER_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        INNER_PARSER.declareObject(ConstructingObjectParser.constructorArg(), ComponentTemplate.PARSER, COMPONENT_TEMPLATE);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), INNER_PARSER, COMPONENT_TEMPLATES);
    }

    private static class NamedComponentTemplate {
        String name;
        ComponentTemplate componentTemplate;

        private NamedComponentTemplate(String name, ComponentTemplate componentTemplate) {
            this.name = name;
            this.componentTemplate = componentTemplate;
        }
    }

    @Override
    public String toString() {
        return "GetIndexTemplatesResponse [indexTemplates=" + componentTemplates + "]";
    }

    private final Map<String, ComponentTemplate> componentTemplates;

    GetComponentTemplatesResponse(Map<String, ComponentTemplate> componentTemplates) {
        this.componentTemplates = Collections.unmodifiableMap(new LinkedHashMap<>(componentTemplates));
    }

    public Map<String, ComponentTemplate> getComponentTemplates() {
        return componentTemplates;
    }


    public static GetComponentTemplatesResponse fromXContent(XContentParser parser) throws IOException {
        return new GetComponentTemplatesResponse(PARSER.apply(parser, null));
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentTemplates);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        GetComponentTemplatesResponse other = (GetComponentTemplatesResponse) obj;
        return Objects.equals(componentTemplates, other.componentTemplates);
    }


}
