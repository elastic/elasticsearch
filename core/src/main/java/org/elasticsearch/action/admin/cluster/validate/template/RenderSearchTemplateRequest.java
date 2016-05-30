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

package org.elasticsearch.action.admin.cluster.validate.template;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.script.Template;

import java.io.IOException;

public class RenderSearchTemplateRequest extends ActionRequest<RenderSearchTemplateRequest> {

    private Template template;
    
    public void template(Template template) {
        this.template = template;
    }
    
    public Template template() {
        return template;
    }
    
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException exception = null;
        if (template == null) {
            exception = new ActionRequestValidationException();
            exception.addValidationError("template must not be null");
        }
        return exception;
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        boolean hasTemplate = template!= null;
        out.writeBoolean(hasTemplate);
        if (hasTemplate) {
            template.writeTo(out);
        }
    }
    
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        if (in.readBoolean()) {
            template = new Template(in);
        }
    }
}
