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
package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.Template;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Facilitates creating template query requests.
 * */
public class TemplateQueryBuilder extends AbstractQueryBuilder<TemplateQueryBuilder> {

    /** Name to reference this type of query. */
    public static final String NAME = "template";

    /** Template to fill. */
    private final Template template;

    static final TemplateQueryBuilder PROTOTYPE = new TemplateQueryBuilder(new Template("proto"));

    /**
     * @param template
     *            the template to use for that query.
     * */
    public TemplateQueryBuilder(Template template) {
        if (template == null) {
            throw new IllegalArgumentException("query template cannot be null");
        }
        this.template = template;
    }

    public Template template() {
        return template;
    }

    /**
     * @param template
     *            the template to use for that query.
     * @param vars
     *            the parameters to fill the template with.
     * @deprecated Use {@link #TemplateQueryBuilder(Template)} instead.
     * */
    @Deprecated
    public TemplateQueryBuilder(String template, Map<String, Object> vars) {
        this(new Template(template, ScriptService.ScriptType.INLINE, null, null, vars));
    }

    /**
     * @param template
     *            the template to use for that query.
     * @param vars
     *            the parameters to fill the template with.
     * @param templateType
     *            what kind of template (INLINE,FILE,ID)
     * @deprecated Use {@link #TemplateQueryBuilder(Template)} instead.
     * */
    @Deprecated
    public TemplateQueryBuilder(String template, ScriptService.ScriptType templateType, Map<String, Object> vars) {
        this(new Template(template, templateType, null, null, vars));
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params builderParams) throws IOException {
        builder.field(TemplateQueryBuilder.NAME);
        template.toXContent(builder, builderParams);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        BytesReference querySource = context.executeQueryTemplate(template, SearchContext.current());
        try (XContentParser qSourceParser = XContentFactory.xContent(querySource).createParser(querySource)) {
            final QueryShardContext contextCopy = new QueryShardContext(context);
            contextCopy.reset(qSourceParser);
            QueryBuilder result = contextCopy.parseContext().parseInnerQueryBuilder();
            context.combineNamedQueries(contextCopy);
            return result.toQuery(context);
        }
    }

    @Override
    protected TemplateQueryBuilder doReadFrom(StreamInput in) throws IOException {
        TemplateQueryBuilder templateQueryBuilder = new TemplateQueryBuilder(Template.readTemplate(in));
        return templateQueryBuilder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        template.writeTo(out);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(template);
    }

    @Override
    protected boolean doEquals(TemplateQueryBuilder other) {
        return Objects.equals(template, other.template);
    }
}
