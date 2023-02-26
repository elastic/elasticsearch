/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.template.get;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public final class TemplateNotFoundException extends ResourceNotFoundException {

    public TemplateNotFoundException(String template) {
        this(template, null);
    }

    public TemplateNotFoundException(String template, Throwable cause) {
        super("no such template [" + template + "]", cause);
        setTemplate(template);
    }

    public TemplateNotFoundException(StreamInput in) throws IOException {
        super(in);
    }
}
