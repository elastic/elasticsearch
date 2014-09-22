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

package org.elasticsearch.action.quality;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Instances of this class represent a complete precision at request. They encode a precision task including search intents and search
 * specifications to be executed subsequently.
 * */
public class QualityRequest extends ActionRequest<QualityRequest> {

    /** The request data to use for evaluation. */
    private QualityTask task;
    
    @Override
    public ActionRequestValidationException validate() {
        // TODO
        return null;
    }

    /** Returns the specification of this qa run including intents to execute, specifications detailing intent translation and metrics
     * to compute. */
    public QualityTask getTask() {
        return task;
    }

    /** Returns the specification of this qa run including intents to execute, specifications detailing intent translation and metrics
     * to compute. */
    public void setTask(QualityTask task) {
        this.task = task;
    }


    @SuppressWarnings("unchecked")
    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        Collection<Intent> intents = new ArrayList<>();
        int intentSize = in.readInt();
        for (int i = 0; i < intentSize; i++) {
            Intent intent = new Intent();
            intent.setIntentId(in.readInt());
            intent.setIntentParameters((Map<String, String>) in.readGenericValue());
            intent.setRatedDocuments((Map<String, Integer>) in.readGenericValue());
            intents.add(intent);
        }
        
        Collection<Specification> specs = new ArrayList<Specification>();
        int specSize = in.readInt();
        for (int i = 0; i < specSize; i++) {

            Specification spec = new Specification();
            Object result = in.readGenericValue();
            spec.setTargetIndices((List<String>) result);
            spec.setSearchRequestTemplate(in.readString());
            spec.setFilter(in.readBytesReference());
            spec.setSpecId(in.readInt());
            specs.add(spec);
        }
        
        String qualityClass = in.readString();
        QualityContext context;
        try {
            context = (QualityContext) Class.forName(qualityClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new IOException("Unable to parse quality parameters from input stream.", e);
        }
        context.read(in);
        
        this.task = new QualityTask();
        task.setIntents(intents);
        task.setSpecifications(specs);
        task.setConfig(context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Collection<Intent> intents = task.getIntents();
        Collection<Specification> specs = task.getSpecifications();
        QualityContext context = task.getQualityContext();
        
        out.writeInt(intents.size());
        for (Intent intent : intents) {
            out.writeInt(intent.getIntentId());
            out.writeGenericValue(intent.getIntentParameters());
            out.writeGenericValue(intent.getRatedDocuments());
        }
        
        out.writeInt(specs.size());
        for (Specification spec : specs) {
            out.writeGenericValue(spec.getTargetIndices());
            out.writeString(spec.getSearchRequestTemplate());
            out.writeBytesReference(spec.getFilter());
            out.writeInt(spec.getSpecId());
        }
        out.writeString(context.getClass().getName());
        context.write(out);
    }
}
