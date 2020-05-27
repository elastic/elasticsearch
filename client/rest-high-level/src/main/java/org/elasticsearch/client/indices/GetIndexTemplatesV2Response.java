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

import org.elasticsearch.cluster.metadata.IndexTemplateV2;
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


public class GetIndexTemplatesV2Response {

    public static final ParseField NAME = new ParseField("name");
    public static final ParseField INDEX_TEMPLATES = new ParseField("index_templates");
    public static final ParseField INDEX_TEMPLATE = new ParseField("index_template");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Map<String, IndexTemplateV2>, Void> PARSER =
        new ConstructingObjectParser<>("index_templates", false,
            a -> ((List<NamedIndexTemplate>) a[0]).stream().collect(Collectors.toMap(n -> n.name, n -> n.indexTemplate,
                (n1, n2) -> n1, LinkedHashMap::new)));

    private static final ConstructingObjectParser<NamedIndexTemplate, Void> INNER_PARSER =
        new ConstructingObjectParser<>("named_index_template", false,
            a -> new NamedIndexTemplate((String) a[0], (IndexTemplateV2) a[1]));

    static {
        INNER_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME);
        INNER_PARSER.declareObject(ConstructingObjectParser.constructorArg(), IndexTemplateV2.PARSER, INDEX_TEMPLATE);
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), INNER_PARSER, INDEX_TEMPLATES);
    }

    private static class NamedIndexTemplate {
        String name;
        IndexTemplateV2 indexTemplate;

        private NamedIndexTemplate(String name, IndexTemplateV2 indexTemplate) {
            this.name = name;
            this.indexTemplate = indexTemplate;
        }
    }

    @Override
    public String toString() {
        return "GetIndexTemplatesResponse [indexTemplates=" + indexTemplates + "]";
    }

    private final Map<String, IndexTemplateV2> indexTemplates;

    GetIndexTemplatesV2Response(Map<String, IndexTemplateV2> indexTemplates) {
        this.indexTemplates = Collections.unmodifiableMap(new LinkedHashMap<>(indexTemplates));
    }

    public Map<String, IndexTemplateV2> getIndexTemplates() {
        return indexTemplates;
    }


    public static GetIndexTemplatesV2Response fromXContent(XContentParser parser) throws IOException {
        return new GetIndexTemplatesV2Response(PARSER.apply(parser, null));
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexTemplates);
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
        GetIndexTemplatesV2Response other = (GetIndexTemplatesV2Response) obj;
        return Objects.equals(indexTemplates, other.indexTemplates);
    }


}
