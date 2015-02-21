/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.template;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class XContentTemplate implements Template {

    public static XContentTemplate YAML = new XContentTemplate(YamlXContent.yamlXContent);

    private final XContent xContent;

    private XContentTemplate(XContent xContent) {
        this.xContent = xContent;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().endObject();
    }

    @Override
    public String render(Map<String, Object> model) {
        try {
            return XContentBuilder.builder(xContent).map(model).bytes().toUtf8();
        } catch (IOException ioe) {
            throw new TemplateException("could not render [" + xContent.type().name() + "] xcontent template", ioe);
        }
    }

}
