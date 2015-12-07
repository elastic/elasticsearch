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


package org.elasticsearch.ingest.processor;

import org.elasticsearch.ingest.IngestDocument;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Processor that executes a list of other "processors". It executes a separate list of
 * "onFailureProcessors" when any of the processors throw an {@link Exception}.
 */
public class CompoundProcessor implements Processor {

    private final List<Processor> processors;
    private final List<Processor> onFailureProcessors;

    public CompoundProcessor(Processor... processor) {
        this(Arrays.asList(processor), Collections.emptyList());
    }
    public CompoundProcessor(List<Processor> processors, List<Processor> onFailureProcessors) {
        this.processors = processors;
        this.onFailureProcessors = onFailureProcessors;
    }

    public List<Processor> getOnFailureProcessors() {
        return onFailureProcessors;
    }

    public List<Processor> getProcessors() {
        return processors;
    }

    @Override
    public String getType() {
        return "compound[" + processors.stream().map(p -> p.getType()).collect(Collectors.joining(",")) + "]";
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        try {
            for (Processor processor : processors) {
                processor.execute(ingestDocument);
            }
        } catch (Exception e) {
            if (onFailureProcessors.isEmpty()) {
                throw e;
            } else {
                executeOnFailure(ingestDocument);
            }
        }

    }

    void executeOnFailure(IngestDocument ingestDocument) throws Exception {
        for (Processor processor : onFailureProcessors) {
            processor.execute(ingestDocument);
        }
    }
}
