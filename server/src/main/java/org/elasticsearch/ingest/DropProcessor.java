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

package org.elasticsearch.ingest;

import java.util.Map;

/**
 * Drop processor only returns {@code null} for the execution result to indicate that any document
 * executed by it should not be indexed.
 */
public final class DropProcessor extends AbstractProcessor {

    public static final String TYPE = "drop";

    private DropProcessor(final String tag) {
        super(tag);
    }

    @Override
    public IngestDocument execute(final IngestDocument ingestDocument) throws Exception {
        return null;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public Processor create(final Map<String, Processor.Factory> processorFactories, final String tag,
            final Map<String, Object> config) {
            return new DropProcessor(tag);
        }
    }
}
